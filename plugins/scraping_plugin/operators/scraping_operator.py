# Filename: plugins/scraping_plugin/operators/scraping_operator.py
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
from bs4 import BeautifulSoup
import csv

class WebScrapingOperator(BaseOperator):
    """
    A custom Airflow operator to perform web scraping.
    """

    @apply_defaults
    def __init__(
        self,
        url: str,
        output_path: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.url = url
        self.output_path = output_path

    def execute(self, context):
        """
        Executes the web scraping task.
        """
        self.log.info(f"Scraping data from {self.url}")
        try:
            response = requests.get(self.url, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            soup = BeautifulSoup(response.content, "html.parser")

            # Example: Scraping book titles and prices from books.toscrape.com
            books = []
            for article in soup.find_all("article", class_="product_pod"):
                title = article.h3.a["title"]
                price = article.find("p", class_="price_color").text
                books.append({"title": title, "price": price})

            # Save the data to a CSV file
            with open(self.output_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["title", "price"])
                writer.writeheader()
                writer.writerows(books)

            self.log.info(f"Successfully scraped {len(books)} books and saved to {self.output_path}")

        except requests.exceptions.RequestException as e:
            self.log.error(f"Error during request: {e}")
            raise
        except Exception as e:
            self.log.error(f"An unexpected error occurred: {e}")
            raise