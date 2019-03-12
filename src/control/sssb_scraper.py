# coding=utf-8
"""Module defining the spider that crawls SSSB website for data acquisition.

"""

__author__ = 'Andres'

import time

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from item_loaders import SSSBApartmentLoader, SSSBApartmentStateLoader
from items import SSSBApartmentItem, SSSBApartmentStateItem


def get_timestamp():
    """Function to get today's date.

    Returns:
         string: today's date in the format YYYYMMDD.

    """

    return time.strftime('%Y-%m-%d %H:%M:%S')


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
    date = get_timestamp()

    def parse(self, response):
        """Method in charge of retrieving relevant data from Bloomberg quotes
            websites via xpaths.

        Args:
            response: HTTP response.

        """

        n = int(response.xpath('(//strong)[1]/text()').extract()[0])
        selectors = response.xpath('//div[contains(@class, \'ObjektIntro\')]')

        for i in range(1, n + 1):
            l = SSSBApartmentLoader(item=SSSBApartmentItem(), selector=selectors[i - 1])
            l.add_xpath('apt_name', './/h4[@class=\'\\"ObjektAdress\\"\']/a/text()')
            l.add_xpath('apt_type', './/h3[@class=\'\\"ObjektTyp\\"\']/a/text()')
            l.add_xpath('apt_zone', '(//dd[@class=\'\\"ObjektOmrade\\"\']/a)[{0}]/text()'.format(i))
            l.add_xpath('apt_price', '(//dd[@class=\'\\"ObjektHyra\\"\'])[{0}]/text()'.format(i))
            l.add_xpath('furnitured', u'.//div[@class=\'\\"ObjektEgenskaper\\"\']/div['
                                      u'@data-title=\'\\"Möblerad\\"\']/span/text()')
            l.add_xpath('electricity', u'.//div[@class=\'\\"ObjektEgenskaper\\"\']/div['
                                       u'@data-title=\'\\"Elström\']/span/text()')
            l.add_xpath('_10_month', u'.//div[@class=\'\\"ObjektEgenskaper\\"\']/div['
                                     u'@data-title=\'\\"10-månadershyra\']/span/text()')
            yield l.load_item()


class SSSBApartmentStateSpider(scrapy.Spider):
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
    date = get_timestamp()

    def parse(self, response):
        """Method in charge of retrieving dynamic data from a current apartment offering.

        Args:
            response: HTTP response.

        """

        n = int(response.xpath('(//strong)[1]/text()').extract()[0])

        for i in range(1, n + 1):
            l = SSSBApartmentStateLoader(item=SSSBApartmentStateItem(), response=response)
            l.add_value('state_timestamp', self.date)
            l.add_xpath('apt_name', '(//h4[@class=\'\\"ObjektAdress\\"\']/a)[{0}]/text()'.format(i))
            l.add_xpath('apt_no_applicants', '(//dd[@class=\'\\"ObjektAntalIntresse\'])[{0}]/text()'.format(i))
            l.add_xpath('apt_top_credits', '(//dd[@class=\'\\"ObjektAntalIntresse\'])[{0}]/text()'.format(i))

            yield l.load_item()


if __name__ == "__main__":
    # logging.getLogger('scrapy').propagate = False
    # logging.getLogger('market_data').propagate = False
    s = get_project_settings()
    process = CrawlerProcess(s)

    spider = SSSBApartmentInfoSpider()
    # spider = SSSBApartmentStateSpider()
    process.crawl(spider)
    process.start()  # the script will block here until the crawling is finished
