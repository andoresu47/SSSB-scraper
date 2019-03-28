"""Script for uploading apartment meta data from the SSSB website.

"""

import datetime
import time
import src.control.functions as fn

if __name__ == '__main__':
    try:
        # Current offer timestamps
        start_date, end_date = fn.get_last_offer_timestamps()
        timezone = start_date.tzinfo
        current_timestamp = datetime.datetime.now()
        current_timestamp = current_timestamp.replace(tzinfo=timezone)

        diff = fn.compute_time_difference(current_timestamp, end_date)

        if 0 < diff < 5:
            time.sleep(300)

        # Current offer size
        db_size = fn.get_db_offering_size()
        live_size = fn.get_live_offering_size()

        # Check if any new apartment has been posted
        if live_size != db_size:
            fn.scrape_apartments()
            fn.scrape_offering()
        fn.send_slack_notification("Apartment offer upload", 1)

    except Exception as e:
        fn.send_slack_notification("Apartment offer upload", 0, str(e))
