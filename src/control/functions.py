from src.control.sssb_scraper import SSSBApartmentInfoSpider, SSSBApartmentStateSpider
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings
import time
import src.data.db_ops as db_connection
from src.data.db_ops import DatabaseException
from src.control.selenium_scraper import SSSBApartmentOffer
import json
import requests
import os
from dotenv import load_dotenv


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
    try:
        sssb_selenium.login()
        sssb_selenium.scrape_offering()

    finally:
        sssb_selenium.close_browser()


def get_live_offering_size():
    sssb_selenium = SSSBApartmentOffer()
    try:
        no_apts = sssb_selenium.get_no_apartments()
        return no_apts

    finally:
        sssb_selenium.close_browser()


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


def send_slack_notification(*args):
    """Function in charge of posting notifications to Slack,
        informing about insertion tasks, or custom messages.

    Args:
        args: If one argument, it gets passed on as JSON to post.
              If two arguments:
                title: title of message to post (which task it is).
                status: 1 for Good, 0 for bad.
              If three arguments:
                Same as with 3, with the following addition:
                error: error raised in case of bad status.
    """

    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    webhook_url = os.getenv("SLACK_WEBHOOK")

    error = ""

    if len(args) == 1 and isinstance(args[0], dict):
        slack_data = args[0]

    elif len(args) >= 2 and isinstance(args[0], str) \
            and isinstance(args[1], int):
        title = args[0]
        status = args[1]

        if len(args) == 3 and isinstance(args[2], str):
            error = args[2]

        if status == 0:
            color = "danger"
            st_text = "Fix me :cry:"
        else:
            color = "good"
            st_text = "Good :tada:"

        slack_data = {"attachments": [
            {
                "fallback": "Apartment data retrieval task monitoring.",
                "color": color,
                "title": title,
                # "thumb_url": ,
                "footer": "Slack API",
                "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png",
                "fields": [
                    {
                        "title": "Status",
                        "value": st_text,
                        "short": False
                    }
                ],
            }
        ]
        }

        if error:
            slack_data.get('attachments')[0]['text'] = error

    else:
        raise TypeError('Incorrect type or number of parameters.')

    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )
