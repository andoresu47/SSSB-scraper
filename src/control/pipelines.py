import os
import time

import datetime

import src.data.db_ops as db_connection
from scrapy.exporters import JsonLinesItemExporter
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from src.data.db_ops import DatabaseException

__author__ = 'Andres'


class SSSBApartmentPipeline(object):
    """Class for processing scraped items and insert them into database.

    """

    def __init__(self):
        """Constructor for initializing connection to database and
            insert counters, as well as spider closed signal.

        """

        # dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

        try:
            db_connection.connect()
        except DatabaseException as e:
            print(str(e))

    def spider_closed(self, spider):
        """Method that sets action to do when the spider is closed,
            in this case, close database connection and send slack
            notification.

        Args:
            spider: spider calling this pipeline.

        """

        db_connection.disconnect()

    def process_item(self, item, spider):
        """Method in charge of validating scraped data and insert it into database.

        Args:
            item: scraped item to validate and insert into database.
            spider: spider calling this pipeline.

        Returns:
            item: validated item, in case everything went smoothly.
                    None, in case DatabaseException was raised when inserting to database.

        """

        apt_name = item['apt_name']
        apt_type = item['apt_type']
        apt_zone = item['apt_zone']
        apt_price = item['apt_price']
        furnitured = item['furnitured'] if 'furnitured' in item else 'False'
        electricity = item['electricity'] if 'electricity' in item else 'False'
        _10_month = item['_10_month'] if '_10_month' in item else 'False'

        try:
            db_connection.set_apartment(apt_name, apt_type, apt_zone, apt_price, furnitured, electricity, _10_month)
            return item

        except DatabaseException as e:
            print("Failure to insert some data: " + str(e))
            return None
