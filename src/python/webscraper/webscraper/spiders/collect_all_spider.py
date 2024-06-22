import scrapy
import time

from datetime import datetime
from scrapy.http import Response

from ..items import WebscraperItem


class AllFinancialDataSpider(scrapy.Spider):
    name = "cas"
    allowed_domains = ["marknadssok.fi.se"]
    start_urls = [
        "https://marknadssok.fi.se/publiceringsklient?page=1",
    ]

    CURRENT_DATE = datetime.today().strftime("%Y-%m-%d")
    MAXIMUM_PAGE_NUMBER = 0
    CURRENT_PAGE_NUMBER = 1

    def parse(self, response: Response):
        self.initialize_spider(response)

        for row in range(0, self.get_table_lenght(response)):
            item = self.extract_item(response, row)

            if item["publication_date"] != self.CURRENT_DATE:
                yield item

        if self.CURRENT_PAGE_NUMBER is not 15:
            next_page_url = f"https://marknadssok.fi.se/publiceringsklient?page={self.CURRENT_PAGE_NUMBER + 1}"
            if next_page_url is not None:
                self.CURRENT_PAGE_NUMBER += 1
                yield response.follow(next_page_url, callback=self.parse)

        time.sleep(2)

    def initialize_spider(self, response: Response):
        """Method holds logic that should only run when on the spiders first run

        Args:
            response (Response): The initial response of the spider
        """
        if self.CURRENT_PAGE_NUMBER == 1:
            self.get_max_page_number(response)

    def get_max_page_number(self, response: Response):
        """Method retrives the maximum amount of pages on the site

        Args:
            response (Response): The response on the website
        """

        self.MAXIMUM_PAGE_NUMBER = int(
            response.xpath(
                '//*[@id="grid-list"]/div[2]/div/div/div[1]/ul/li[14]/a/text()'
            )
            .get()
            .replace("s", "")
        )

    def get_table_lenght(self, response: Response):
        """The method gets the lenght of the current table

        Args:
            response (Response): The response of the current request

        Returns:
            int: The lenght of the table
        """
        return len(
            response.xpath('//*[@id="grid-list"]/div[1]/div/table/tbody/tr').getall()
        )

    def extract_item(self, response: Response, row: int):
        """Method collects data for an item

        Args:
            response (Response): The response from the current page

        Returns:
            WebscraperItem: The data that have been scraped
        """

        item = WebscraperItem()

        item["publication_date"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[1]/text()'
        ).extract_first()
        item["issuer"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[2]/text()'
        ).extract_first()
        item["name"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[3]/text()'
        ).extract_first()
        item["role"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[4]/text()'
        ).extract_first()
        item["related"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[5]/text()'
        ).extract_first()
        item["nature_of_purchase"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[6]/text()'
        ).extract_first()
        item["instrument_name"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[7]/text()'
        ).extract_first()
        item["instrument_type"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[8]/text()'
        ).extract_first()
        item["isin"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[9]/text()'
        ).extract_first()
        item["transaction_date"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[10]/text()'
        ).extract_first()
        item["volume"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[11]/text()'
        ).extract_first()
        item["volume_unit"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[12]/text()'
        ).extract_first()
        item["price"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[13]/text()'
        ).extract_first()
        item["curreny"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[14]/text()'
        ).extract_first()

        return item
