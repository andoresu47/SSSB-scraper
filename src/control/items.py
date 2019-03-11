import scrapy

__author__ = 'Andres'


class SSSBApartmentItem(scrapy.Item):
    """Class for defining items for SSSB Apartment Spider.

    """

    apt_name = scrapy.Field()
    apt_type = scrapy.Field()
    apt_zone = scrapy.Field()
    apt_price = scrapy.Field()
    furnitured = scrapy.Field()
    electricity = scrapy.Field()
    _10_month = scrapy.Field()


class SSSBApartmentStateItem(scrapy.Item):
    """Class for defining items for SSSB State Spider.

    """

    # From the timestamp we can get the current offer
    state_timestamp = scrapy.Field()
    # From the appartment name we can get its internal id
    apt_name = scrapy.Field()
    apt_no_applicants = scrapy.Field()
    apt_top_credits = scrapy.Field()
