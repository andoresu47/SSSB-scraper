import datetime
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join

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

    if not x.strip('-'):
        return None
    else:
        return x


class SSSBApartmentLoader(ItemLoader):
    """Class for loading items for SSSBApartmentSpider.

    """

    # Checks if parsed data is empty, in which case, the item is set to None
    default_input_processor = MapCompose(unicode.strip, is_empty)

    default_output_processor = Join()


class SSSBApartmentStateLoader(ItemLoader):
    """Class for loading items for SSSBApartmentStateSpider.

    """

    # Checks if parsed data is empty, in which case, the item is set to None
    default_input_processor = MapCompose(unicode.strip, is_empty)

    default_output_processor = Join()
