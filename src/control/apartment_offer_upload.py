import src.control.functions as fn

if __name__ == '__main__':
    try:
        # Current offer size
        db_size = fn.get_db_offering_size()
        live_size = fn.get_live_offering_size()

        # Check if any new apartment has been posted
        if live_size != db_size:
            fn.scrape_apartments()
            fn.scrape_offering()
        fn.send_slack_notification(error=False)

    except Exception as e:
        fn.send_slack_notification(error=True)
