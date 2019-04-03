import datetime
import math

import src.control.functions as fn
from dotenv import load_dotenv
import os


def single_rooms(credits):
    offer_id = fn.get_last_offer_id()
    fn.gen_plots_single_room(offer_id, credits)

    filepath1 = os.path.join(os.path.dirname(__file__), "topCredits.png")
    title1 = "Single Rooms: Top credits"

    filepath2 = os.path.join(os.path.dirname(__file__), "numberOfApplicants.png")
    title2 = "Single Rooms: Number of applicants"

    fn.post_slack_image(filepath1, title1)
    fn.post_slack_image(filepath2, title2)


def kitchenette(credits):
    offer_id = fn.get_last_offer_id()
    fn.gen_plots_kitchenette(offer_id, credits)

    filepath1 = os.path.join(os.path.dirname(__file__), "topCredits.png")
    title1 = "Rooms with kitchenette: Top credits"

    filepath2 = os.path.join(os.path.dirname(__file__), "numberOfApplicants.png")
    title2 = "Rooms with kitchenette: Number of applicants"

    fn.post_slack_image(filepath1, title1)
    fn.post_slack_image(filepath2, title2)


if __name__ == '__main__':

    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    start_date = os.getenv("SSSB_START")
    start_date_timestamp = datetime.datetime.strptime(start_date, "%d-%m-%Y")

    current_timestamp = datetime.datetime.now()

    td = current_timestamp - start_date_timestamp

    own_credit_days = int(math.floor(td.total_seconds() / (24 * 3600)))

    # single_rooms(own_credit_days)
    kitchenette(own_credit_days)
