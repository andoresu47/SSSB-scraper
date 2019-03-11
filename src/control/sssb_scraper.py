"""Module defining the spider that crawls SSSB website for data acquisition.

"""

__author__ = 'Andres'

import time

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from item_loaders import SSSBApartmentLoader, SSSBApartmentStateLoader
from items import SSSBApartmentItem, SSSBApartmentStateItem


def get_date():
    """Function to get today's date.

    Returns:
         string: today's date in the format YYYYMMDD.

    """

    return time.strftime("%Y%m%d").decode('utf-8')


class SSSBApartmentInfoSpider(scrapy.Spider):
    """Class for scraping SSSB apartment offerings meta data.

    """

    name = "sssb_apt_spider"
    start_urls = ['https://www.sssb.se/widgets/?paginationantal=all&callback=jQuery17208255315905375711_1549963009511'
                  '&widgets%5B%5D=alert&widgets%5B%5D=objektsummering%40lagenheter&widgets%5B%5D=objektfilter'
                  '%40lagenheter&widgets%5B%5D=objektsortering%40lagenheter&widgets%5B%5D=objektlistabilder'
                  '%40lagenheter&widgets%5B%5D=paginering%40lagenheter&widgets%5B%5D=pagineringantal%40lagenheter'
                  '&widgets%5B%5D=pagineringgofirst%40lagenheter&widgets%5B%5D=pagineringgonew%40lagenheter&widgets'
                  '%5B%5D=pagineringlista%40lagenheter&widgets%5B%5D=pagineringgoold%40lagenheter&widgets%5B%5D'
                  '=pagineringgolast%40lagenheter']
    date = get_date()

    def parse(self, response):
        """Method in charge of retrieving relevant data from Bloomberg quotes
            websites via xpaths.

        Args:
            response: HTTP response.

        """

        n = int(response.xpath('(//strong)[1]/text()').extract()[0])

        for i in range(1, n + 1):
            l = SSSBApartmentLoader(item=SSSBApartmentItem(), response=response)
            l.add_xpath('apt_name', '(//h4[@class=\'\\"ObjektAdress\\"\']/a)[{0}]/text()'.format(i))
            l.add_xpath('apt_type', '(//h3[@class=\'\\"ObjektTyp\\"\']/a)[{0}]/text()'.format(i))
            l.add_xpath('apt_zone', '(//dd[@class=\'\\"ObjektOmrade\\"\']/a)[{0}]/text()'.format(i))
            yield l.load_item()


class SSSBApartmentState(scrapy.Spider):
    """Class for scraping SSSB apartments dynamic info.

    """

    name = "sssb_st_spider"
    start_urls = ['https://www.sssb.se/widgets/?paginationantal=all&callback=jQuery17208255315905375711_1549963009511'
                  '&widgets%5B%5D=alert&widgets%5B%5D=objektsummering%40lagenheter&widgets%5B%5D=objektfilter'
                  '%40lagenheter&widgets%5B%5D=objektsortering%40lagenheter&widgets%5B%5D=objektlistabilder'
                  '%40lagenheter&widgets%5B%5D=paginering%40lagenheter&widgets%5B%5D=pagineringantal%40lagenheter'
                  '&widgets%5B%5D=pagineringgofirst%40lagenheter&widgets%5B%5D=pagineringgonew%40lagenheter&widgets'
                  '%5B%5D=pagineringlista%40lagenheter&widgets%5B%5D=pagineringgoold%40lagenheter&widgets%5B%5D'
                  '=pagineringgolast%40lagenheter']
    date = get_date()

    def parse(self, response):
        """Method in charge of retrieving dynamic data from a current apartment offering.

        Args:
            response: HTTP response.

        """


if __name__ == "__main__":
    # logging.getLogger('scrapy').propagate = False
    # logging.getLogger('market_data').propagate = False
    s = get_project_settings()
    process = CrawlerProcess(s)

    spider = SSSBApartmentInfoSpider()
    process.crawl(spider)
    process.start()  # the script will block here until the crawling is finished
