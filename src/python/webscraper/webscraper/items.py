import scrapy


class WebscraperItem(scrapy.Item):
    publication_date = scrapy.Field()
    issuer = scrapy.Field()
    name = scrapy.Field()
    role = scrapy.Field()
    related = scrapy.Field()
    nature_of_purchase = scrapy.Field()
    instrument_name = scrapy.Field()
    instrument_type = scrapy.Field()
    isin = scrapy.Field()
    transaction_date = scrapy.Field()
    volume = scrapy.Field()
    volume_unit = scrapy.Field()
    price = scrapy.Field()
    curreny = scrapy.Field()
