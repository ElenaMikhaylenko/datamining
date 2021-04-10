from time import sleep
import datetime as dt
from urllib.parse import urljoin
import requests
import bs4
import pymongo


MONTHS = {
    "янв": 1,
    "фев": 2,
    "мар": 3,
    "апр": 4,
    "май": 5,
    "мая": 5,
    "июн": 6,
    "июл": 7,
    "авг": 8,
    "сен": 9,
    "окт": 10,
    "ноя": 11,
    "дек": 12,
}


class MagnitParse:
    def __init__(self, start_url, db_client):
        self.start_url = start_url
        self.db = db_client["gb_data_mining_29_03_2021"]
        self.collection = self.db["magnit_products"]

    def _get_response(self, url):
        response = requests.get(url)
        while response.status_code != 200:
            response = requests.get(url)
            sleep(0.5)
        return requests.get(url)

    def _get_soup(self, url):
        response = self._get_response(url)
        return bs4.BeautifulSoup(response.text, "lxml")

    def run(self):
        soup = self._get_soup(self.start_url)
        catalog = soup.find("div", attrs={"class": "сatalogue__main"})
        for prod_a in catalog.find_all("a", recursive=False):
            product_data = self._parse(prod_a)
            self._save(product_data)

    @property
    def template(self):
        current_year = dt.datetime.now().year
        return {
            "product_name": lambda a: a.find(
                "div", attrs={"class": "card-sale__title"}
            ).text,
            "url": lambda a: urljoin(self.start_url, a.attrs.get("href", "")),
            "promo_name": lambda a: a.find(
                "div", attrs={"class": "card-sale__name"}
            ).text,
            "old_price": lambda a: float(
                ".".join(
                    itm
                    for itm in a.find(
                        "div", attrs={"class": "label__price_old"}
                    ).text.split()
                )
            ),
            "new_price": lambda a: float(
                ".".join(
                    itm
                    for itm in a.find(
                        "div", attrs={"class": "label__price_new"}
                    ).text.split()
                )
            ),
            "image_url": lambda a: urljoin(
                self.start_url, a.find("img").attrs.get("data-src")
            ),
            "date_from": lambda a: self._get_date(
                a.find("div", attrs={"class": "card-sale__date"}).text,
                current_year,
            )[0],
            "date_to": lambda a: self._get_date(
                a.find("div", attrs={"class": "card-sale__date"}).text,
                current_year,
            )[1],
        }

    def _get_month(self, month_str: str) -> int:
        for month, month_number in MONTHS.items():
            if month in month_str:
                return month_number
        raise Exception(f"Unknown month: {month_str}")

    def _get_date(self, date_string: str, current_year: int) -> list:
        data = (
            date_string.lower()
            .replace("только", "")
            .replace("с", "")
            .replace("по", "")
            .replace("до", "")
            .replace("\n", "")
            .strip()
            .split(" ")
        )
        from_ = dt.datetime(
            current_year, self._get_month(data[1]), int(data[0])
        )
        if len(data) == 2:
            to_ = from_
        else:
            to_ = dt.datetime(
                current_year, self._get_month(data[3]), int(data[2])
            )
        if (to_.month < from_.month) or (
            to_.month == from_.month and to_.day < from_.day
        ):
            to_.year = to_.year + 1
        return [from_, to_]

    def _parse(self, product_a) -> dict:
        data = {}
        for key, funk in self.template.items():
            try:
                data[key] = funk(product_a)
            except (AttributeError, ValueError):
                pass
        return data

    def _save(self, data: dict):
        self.collection.insert_one(data)


if __name__ == "__main__":
    url = "https://magnit.ru/promo/"
    db_client = pymongo.MongoClient("mongodb://localhost:27017")
    parser = MagnitParse(url, db_client)
    parser.run()
