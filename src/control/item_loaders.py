import datetime
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join
import re

__author__ = 'Andres'


def is_empty(x):
    """Function to decide if parameter is a single dash (empty)
        or not.

    Args:
        x: string to validate.

    Returns:
         string: represents recieved parameter, in case it's
            not empty. None if empty.

    """

    if not x:
        return None
    else:
        return x


def get_first_space(x):
    """Function to get the first element from a space splitting.

    Args:
        x: string to split.

    Returns:
        string: first element of space splitting.

    """

    return x.split()[0]


def get_second_space(x):
    """Function to get the second element from a space splitting.

    Args:
        x: string to split.

    Returns:
        string: second element of space splitting.

    """

    return x.split()[1]


def get_number(x):
    return re.findall(r'\d+', x)[0]


def set_boolean(x):
    if x:
        return "True"
    else:
        return "False"


class SSSBApartmentLoader(ItemLoader):
    """Class for loading items for SSSBApartmentSpider.

    """

    # Checks if parsed data is empty, in which case, the item is set to None
    default_input_processor = MapCompose(unicode.strip, is_empty)

    # Parsing of price
    apt_price_in = MapCompose(get_first_space)
    apt_price_out = TakeFirst()

    # Parsing of furnitured
    furnitured_in = MapCompose(set_boolean)
    furnitured_out = TakeFirst()

    # Parsing of electricity
    electricity_in = MapCompose(set_boolean)
    electricity_out = TakeFirst()

    # Parsing of _10_month
    _10_month_in = MapCompose(set_boolean)
    _10_month_out = TakeFirst()

    default_output_processor = Join()


class SSSBApartmentStateLoader(ItemLoader):
    """Class for loading items for SSSBApartmentStateSpider.

    """

    # Checks if parsed data is empty, in which case, the item is set to None
    default_input_processor = MapCompose(unicode.strip, is_empty)

    apt_no_applicants_in = MapCompose(get_second_space, get_number)
    apt_no_applicants_out = TakeFirst()

    apt_top_credits_in = MapCompose(get_first_space)
    apt_top_credits_out = TakeFirst()

    default_output_processor = Join()
