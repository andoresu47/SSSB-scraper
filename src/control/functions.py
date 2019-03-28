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
from matplotlib import pyplot as plt
from slackclient import SlackClient


def get_timestamp():
    """Function to get today's date.

    Returns:
         string: today's date in the format YYYYMMDD.
    """

    return time.strftime('%Y-%m-%d %H:%M:%S')


def compute_time_difference(tstamp1, tstamp2):
    """Function to compute the difference between two time stamps in minutes.

    Args:
        tstamp1: first timestamp.
        tstamp2: second timestamp.

    Returns:
        int: integer representing the time difference in minutes.
    """

    td = tstamp1 - tstamp2
    return int(round(td.total_seconds() / 60))


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


def get_last_offer_id():
    """Function to get the id of the current offer.

    Returns:
        int = id of the desired offer.
    """

    db_conn_status = db_connection.is_connected()

    if not db_conn_status:
        db_connection.connect()

    try:
        offer_id = db_connection.get_current_offer_id()
        return offer_id

    except DatabaseException as e:
        print(str(e))

    finally:
        if not db_conn_status:
            db_connection.disconnect()


def scrape_apartments():
    """ Function to scrape meta data about available apartments via a Scrapy spider,
    which inserts the acquired data into a database.
    """

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
    """ Function to scrape dynamic data about available apartments via a Scrapy spider,
        which inserts the acquired data into a database.
    """

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
    """ Function to scrape timing data about an apartment offering via a Selenium crawler,
        which inserts the acquired data into a database.
    """

    sssb_selenium = SSSBApartmentOffer()
    try:
        sssb_selenium.login()
        sssb_selenium.scrape_offering()

    finally:
        sssb_selenium.close_browser()


def get_live_offering_size():
    """ Function to get the number of apartments currently being offered from the
    SSSB website.

    Returns:
        int: number of apartments in live offering.
    """

    sssb_selenium = SSSBApartmentOffer()
    try:
        no_apts = sssb_selenium.get_no_apartments()
        return no_apts

    finally:
        sssb_selenium.close_browser()


def get_db_offering_size():
    """ Function to get the number of apartments currently stored in the database
    fr the last available offering.

    Returns:
        int: number of apartments in last db-inserted offering.
    """

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


def get_offered_apartments(offer_id):
    """ Function to get a list of apartment ids for a given offer.

    Args:
        offer_id: id of desired apartment offering.

    Returns:
        list: list containing the ids of the desired apartments.
    """

    db_conn_status = db_connection.is_connected()

    if not db_conn_status:
        db_connection.connect()

    try:
        apt_list = db_connection.get_offered_apartments(offer_id)
        return apt_list

    except DatabaseException as e:
        print(str(e))

    finally:
        if not db_conn_status:
            db_connection.disconnect()


def get_top_credits_hist(offer_id, apartment_list):
    """Function to get the time series of the top credits for the specified offer and apartments.

    Args:
        offer_id: id of desired apartment offering.
        apartment_list: list of apartment ids whose info we want to retrieve.

    Returns:
        pandas.DataFrame: data frame containing the "top_credits" times series for each apartment.
    """

    db_conn_status = db_connection.is_connected()

    if not db_conn_status:
        db_connection.connect()

    try:
        df = db_connection.get_all_top_credits(offer_id, apartment_list)
        return df

    except DatabaseException as e:
        print(str(e))

    finally:
        if not db_conn_status:
            db_connection.disconnect()


def get_applicants_hist(offer_id, apartment_list):
    """Function to get the time series of the number of applicants for the specified offer and apartments.

    Args:
        offer_id: id of desired apartment offering.
        apartment_list: list of apartment ids whose info we want to retrieve.

    Returns:
        pandas.DataFrame: data frame containing the "no_applicants" times series for each apartment.
    """

    db_conn_status = db_connection.is_connected()

    if not db_conn_status:
        db_connection.connect()

    try:
        df = db_connection.get_all_no_applicants(offer_id, apartment_list)
        return df

    except DatabaseException as e:
        print(str(e))

    finally:
        if not db_conn_status:
            db_connection.disconnect()


def get_offered_apartments_by_type(offer_id, type):
    """ Function to get a list of apartment ids having a given type for the given offer.

    Args:
        offer_id: id of desired apartment offering.
        type: apartment type to use for filtering results.

    Returns:
        list: list containing the ids of the desired apartments.
    """

    db_conn_status = db_connection.is_connected()

    if not db_conn_status:
        db_connection.connect()

    try:
        apt_list = db_connection.get_offered_apartments_by_type(offer_id, type)
        return apt_list

    except DatabaseException as e:
        print(str(e))

    finally:
        if not db_conn_status:
            db_connection.disconnect()


def plot_time_series(series, data=None, own_credits=None):
    """ Function to generate a plot of the top credits or number of applicants time series.

    Args:
        series: boolean representing the nature of the data (True for "credits" and False for "applicants").
        data: id of desired apartment offering.
        own_credits: current credit days.

    Returns:
        pandas.DataFrame: data frame containing the specified times series for each apartment.
    """
    light_gray = (225 / 255., 225 / 255., 225 / 255.)
    lightest_gray = (250 / 255., 250 / 255., 250 / 255.)

    f = plt.figure(figsize=(12, 6))

    if series:
        plt.title('Top credits', fontsize=18)
        filename = "topCredits.png"
    else:
        plt.title('Number of applicants', fontsize=18)
        filename = "numberOfApplicants.png"

    if data is None:
        last_offer_id = get_last_offer_id()
        apts = get_offered_apartments(last_offer_id)

        if series:
            data = get_top_credits_hist(last_offer_id, apts)
        else:
            data = get_applicants_hist(last_offer_id, apts)

    data = data.fillna(method='ffill')
    data.plot(ax=f.gca())
    lgd = plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
    plt.grid(b=True, color=light_gray, linestyle='-')

    ax = plt.gca()
    ax.set_facecolor(lightest_gray)

    if series and own_credits is not None:
        plt.axhline(own_credits, color='k', linestyle='--')

    f.savefig(filename, bbox_extra_artists=(lgd,), bbox_inches='tight')

    return f


def gen_plots_kitchenette(offer_id, own_credits):
    knt = get_offered_apartments_by_type(offer_id, 'Ett rum med pentry')

    df1 = get_top_credits_hist(offer_id, knt)
    df2 = get_applicants_hist(offer_id, knt)
    data1 = df1.fillna(method='ffill')
    data2 = df2.fillna(method='ffill')

    f = plot_time_series(True, data1, own_credits)
    g = plot_time_series(False, data2, own_credits)

    return f, g


def post_slack_image(filepath, title):
    """Function in charge of posting notifications to Slack's channel "sssb",
    in the form of apartment data plots.

    """

    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    slack_token = os.getenv("SLACK_API_TOKEN")

    sc = SlackClient(slack_token)

    with open(filepath, 'rb') as file_content:
        sc.api_call(
            "files.upload",
            channels="#sssb_sketch",
            file=file_content,
            title=title,
            username='SlackBot',
        )


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


if __name__ == '__main__':
    post_slack_image('topCredits.png')
