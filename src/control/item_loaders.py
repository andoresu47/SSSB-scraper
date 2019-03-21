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
    """Function to extract a number out from a string which contains it.

    Args:
        x: string from which to extract the contained number.

    Returns:
        string: number contained in string.

    """

    return re.findall(r'\d+', x)[0]


def set_boolean(x):
    """Function to assert if a string is empty or not.

    Args:
        x: possibly empty string.

    Returns:
        string: "true" or "false", depending on the input string.

    """

    if x:
        return "True"
    else:
        return "False"


def remove_extra_middle_spaces(x):
    """Function to remove redundant whitespaces in input string.

    Args:
        x: string to strip.

    Returns:
        string: string without redundant whitespaces.

    """

    return " ".join(x.split())


class SSSBApartmentLoader(ItemLoader):
    """Class for loading items for SSSBApartmentSpider.

    """

    # Checks if parsed data is empty, in which case, the item is set to None
    default_input_processor = MapCompose(str.strip, is_empty)

    # Parsing of apartment address (name)
    apt_name_in = MapCompose(remove_extra_middle_spaces)
    apt_name_out = TakeFirst()

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
    default_input_processor = MapCompose(str.strip, is_empty)

    apt_no_applicants_in = MapCompose(get_second_space, get_number)
    apt_no_applicants_out = TakeFirst()

    apt_top_credits_in = MapCompose(get_first_space)
    apt_top_credits_out = TakeFirst()

    default_output_processor = Join()
