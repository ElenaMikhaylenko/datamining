import time
import json
from pathlib import Path
from typing import Dict, List

import requests


class Parse5ka:
    headers = {"User-Agent": "Oomph!"}

    def __init__(self, start_url: str, save_path: Path):
        self.start_url = start_url
        self.save_path = save_path

    def _get_response(self, url: str):
        while True:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response
            time.sleep(0.5)

    def run(self):
        for product in self._parse(self.start_url):
            product_path = self.save_path.joinpath(f"{product['id']}.json")
            self._save(product, product_path)

    def _parse(self, url: str):
        while url:
            response = self._get_response(url)
            data: Dict = response.json()
            url = data["next"]
            for product in data["results"]:
                yield product

    def _save(self, data: Dict, file_path: Path):
        file_path.write_text(
            json.dumps(data, ensure_ascii=False), encoding="utf-8"
        )


class CategoriesParser(Parse5ka):
    def __init__(self, categories_url: str, *args, **kwargs):
        self.categories_url = categories_url
        super().__init__(*args, **kwargs)

    def get_categories(self) -> List[Dict]:
        return self._get_response(self.categories_url).json()

    def run(self) -> None:
        for cat in self.get_categories():
            cat["products"] = []
            products = self._parse(
                f"{self.start_url}" f"?categories={cat['parent_group_code']}"
            )
            cat["products"].extend(products)
            self._save(
                cat,
                self.save_path.joinpath(
                    f"{cat['parent_group_code']}_{cat['parent_group_name']}.json"
                ),
            )


def get_save_path(dir_name):
    save_path = Path(__file__).parent.joinpath(dir_name)
    if not save_path.exists():
        save_path.mkdir()
    return save_path


if __name__ == "__main__":
    url = "https://5ka.ru/api/v2/special_offers/"
    cat_url = "https://5ka.ru/api/v2/categories/"
    save_path_categories = get_save_path("categories")
    cat_parser = CategoriesParser(cat_url, url, save_path_categories)
    cat_parser.run()
