from src.control.sssb_scraper import SSSBApartmentInfoSpider, SSSBApartmentStateSpider
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings
import time
import src.data.db_ops as db_connection
from src.data.db_ops import DatabaseException
from src.control.selenium_scraper import SSSBApartmentOffer


def get_timestamp():
    """Function to get today's date.

    Returns:
         string: today's date in the format YYYYMMDD.

    """

    return time.strftime('%Y-%m-%d %H:%M:%S')


def get_last_offer_timestamps():
    """Function to get the start and end timestamps of the current offer.

    Returns:
        datetime.datetime, datetime.datetime = start and ending timestamps respectively.
    """

    db_conn_status = db_connection.is_connected()

    if not db_conn_status:
        db_connection.connect()

    try:
        start_date, end_date = db_connection.get_current_offer_dates()
        return start_date, end_date

    except DatabaseException as e:
        print(str(e))

    finally:
        if not db_conn_status:
            db_connection.disconnect()


def scrape_apartments():
    s = get_project_settings()
    s.update({
        'LOG_ENABLED': False,
        'LOG_ENCODING ': None,
        'ITEM_PIPELINES': {
            'pipelines.SSSBApartmentPipeline': 400,
        }
    })

    process = CrawlerProcess(s)

    spider = SSSBApartmentInfoSpider()
    process.crawl(spider)
    process.start()


def scrape_apartment_states():
    s = get_project_settings()
    s.update({
        'LOG_ENABLED': False,
        'LOG_ENCODING ': None,
        'ITEM_PIPELINES': {
            'pipelines.SSSBApartmentStatePipeline': 400,
        }
    })

    process = CrawlerProcess(s)

    spider = SSSBApartmentStateSpider()
    process.crawl(spider)
    process.start()


def scrape_offering():
    sssb_selenium = SSSBApartmentOffer()
    sssb_selenium.login()
    sssb_selenium.scrape_offering()
    sssb_selenium.close_browser()


def get_live_offering_size():
    sssb_selenium = SSSBApartmentOffer()
    no_apts = sssb_selenium.get_no_apartments()
    sssb_selenium.close_browser()
    return no_apts


def get_db_offering_size():
    db_conn_status = db_connection.is_connected()

    if not db_conn_status:
        db_connection.connect()

    try:
        size = db_connection.get_current_offer_size()
        return size

    except DatabaseException as e:
        print(str(e))

    finally:
        if not db_conn_status:
            db_connection.disconnect()
