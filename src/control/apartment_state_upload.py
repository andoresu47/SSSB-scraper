import datetime
import src.control.functions as fn

if __name__ == '__main__':
    # Current offer timestamps
    start_date, end_date = fn.get_last_offer_timestamps()
    timezone = start_date.tzinfo
    current_timestamp = datetime.datetime.now()
    current_timestamp = current_timestamp.replace(tzinfo=timezone)

    if start_date <= current_timestamp <= end_date:
        fn.scrape_apartment_states()
    elif end_date < current_timestamp:
        fn.scrape_apartments()
        fn.scrape_offering()
    else:
        raise Exception("Error in timestamps")
