import scrapy

from datetime import datetime, timedelta
from scrapy.http import Response
from scrapy.exceptions import CloseSpider

from ..items import WebscraperItem


class AllFinancialDataSpider(scrapy.Spider):
    name = "cas"
    allowed_domains = ["marknadssok.fi.se"]
    start_urls = [
        "https://marknadssok.fi.se/publiceringsklient?page=",
    ]

    def __init__(
        self,
        start_date: str = None,
        end_date: str = None,
        page_jump: int = None,
        *args,
        **kwargs,
    ):
        super(AllFinancialDataSpider, self).__init__(*args, **kwargs)

        self.COLLECTED_MAX_PAGES = False

        self.TODAY = datetime.today()
        self.START_DATE = (
            self._parse_date(start_date) if start_date else self._default_start_date()
        )
        self.END_DATE = (
            self._parse_date(end_date) if end_date else self._default_end_date()
        )

        self.MAXIMUM_PAGE_NUMBER = None
        self.CURRENT_PAGE_NUMBER = (
            1 if page_jump is None else self._validate_page_jump(page_jump)
        )

        self.start_urls[0] = self.start_urls[0] + str(self.CURRENT_PAGE_NUMBER)
        self.download_delay = 2

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

    def _validate_page_jump(self, page_jump):
        """Validate and return the page jump value as a positive integer."""
        try:
            page_number = int(page_jump)
            if page_number < 1:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError(
                f"Invalid page_jump: {page_jump}. Must be a positive integer."
            )
        return page_number

    def parse(self, response: Response):
        """Method is in charge of processing the response and returning scraped data and/or more URLs to follow.

        Args:
            response (Response): The response to parse

        Raises:
            CloseSpider: When the end-date is reached (or last page) the program is shut down

        Yields:
            scrapy.Item: Current scrapy item
        """
        self.set_max_page_number(response)

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

    def set_max_page_number(self, response: Response):
        """Method sets the maximum amount of pages on the site

        Args:
            response (Response): The response on the website
        """
        if self.COLLECTED_MAX_PAGES is False:
            if self.CURRENT_PAGE_NUMBER < 8:
                index = 14
            elif (
                self.CURRENT_PAGE_NUMBER > (self.MAXIMUM_PAGE_NUMBER or 0) - 9
                and (self.MAXIMUM_PAGE_NUMBER or 0) > -9
            ):
                index = 16
            else:
                index = 15

            xpath = (
                f'//*[@id="grid-list"]/div[2]/div/div/div[1]/ul/li[{index}]/a/text()'
            )

            page_number_str = response.xpath(xpath).get().replace("s", "")
            self.MAXIMUM_PAGE_NUMBER = int(page_number_str)
            self.COLLECTED_MAX_PAGES = True

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
