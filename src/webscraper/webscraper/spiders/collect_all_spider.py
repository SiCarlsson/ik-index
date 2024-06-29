import scrapy

from datetime import datetime, timedelta
from scrapy.http import Response
from scrapy.exceptions import CloseSpider

from ..items import WebscraperItem


class AllFinancialDataSpider(scrapy.Spider):
    name = "cas"
    allowed_domains = ["marknadssok.fi.se"]
    start_urls = [
        "https://marknadssok.fi.se/publiceringsklient?page=1",
    ]

    def __init__(
        self,
        start_date: str = None,
        end_date: str = None,
        *args,
        **kwargs,
    ):
        super(AllFinancialDataSpider, self).__init__(*args, **kwargs)
        self.download_delay = 2

        self.TODAY = datetime.today()

        self.START_DATE = (
            self._parse_date(start_date) if start_date else self._default_start_date()
        )
        self.END_DATE = (
            self._parse_date(end_date) if end_date else self._default_end_date()
        )

        self.CURRENT_PAGE_NUMBER = 1

    def _parse_date(self, date_str: str):
        """Method parses the date and ensures correct fomatting

        Args:
            date_str (str): A string containing a date in the format YYYY-MM-DD

        Raises:
            ValueError: If the date format is not followed

        Returns:
            str: The date as a string in the correct format
        """
        try:
            year, month, day = map(int, date_str.split("-"))
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            raise ValueError(
                f"Invalid date format: {date_str}. Expected format: 'YYYY-MM-DD'."
            )

    def _default_start_date(self):
        """Contains the default start date

        Returns:
            str: Default start date
        """
        return (self.TODAY - timedelta(days=1)).strftime("%Y-%m-%d")

    def _default_end_date(self):
        """Conatins the default end date

        Returns:
            str: Default end date
        """
        return datetime(2000, 1, 1).strftime("%Y-%m-%d")

    def parse(self, response: Response):
        """Method is in charge of processing the response and returning scraped data and/or more URLs to follow.

        Args:
            response (Response): The response to parse

        Raises:
            CloseSpider: When the end-date is reached (or last page) the program is shut down

        Yields:
            scrapy.Item: Current scrapy item
        """
        self.initialize_spider(response)

        for row in range(0, self.get_table_lenght(response)):
            item = self.extract_item(response, row)

            if item["publication_date"] == self.END_DATE:
                raise CloseSpider("End date reached!")

            if item["publication_date"] <= self.START_DATE:
                yield item

        if self.CURRENT_PAGE_NUMBER < self.MAXIMUM_PAGE_NUMBER:
            next_page_url = f"https://marknadssok.fi.se/publiceringsklient?page={self.CURRENT_PAGE_NUMBER + 1}"
            if next_page_url is not None:
                self.CURRENT_PAGE_NUMBER += 1
                yield response.follow(next_page_url, callback=self.parse)
        else:
            raise CloseSpider("Maximum page reached!")

    def initialize_spider(self, response: Response):
        """Method holds logic that should only run once before the spider starts scraping

        Args:
            response (Response): The initial response of the spider
        """
        if self.CURRENT_PAGE_NUMBER == 1:
            self.MAXIMUM_PAGE_NUMBER = self.get_max_page_number(response)

    def get_max_page_number(self, response: Response):
        """Method gets and sets the maximum amount of pages on the site

        Args:
            response (Response): The response on the website
        """

        return int(
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
        item["currency"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[14]/text()'
        ).extract_first()
        item["status"] = response.xpath(
            f'//*[@id="grid-list"]/div[1]/div/table/tbody/tr[{row + 1}]/td[15]/a/text()'
        ).extract_first()

        return item
