"""Module defining the Selenium crawler that retrieves logged-in data from SSSB.

"""

__author__ = 'Andres'

from selenium import webdriver
from dotenv import load_dotenv
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import src.data.db_ops as db_connection
from src.data.db_ops import DatabaseException

SSSB_FRONTPAGE = 'https://www.sssb.se/en'
SSSB_AVAILABLE_APARTMENTS = 'https://www.sssb.se/en/find-apartment/apply-for-apartment/available-apartments' \
                            '/?paginationantal=all '


class ApartmentException(Exception):
    """Class for managing apartment info retrieval exceptions.

    """

    pass


class SSSBApartmentOffer:
    def __init__(self):
        dirname = os.path.dirname(__file__)
        # Headless browser
        options = webdriver.FirefoxOptions()
        options.add_argument('-headless')
        self.browser = webdriver.Firefox(executable_path=os.path.join(dirname, '../../Dependencies/Selenium/geckodriver'),
                                         options=options)
        self.browser.set_page_load_timeout(30)
        self.browser.get(SSSB_FRONTPAGE)

        self.logged_in = False

    def login(self):
        dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
        load_dotenv(dotenv_path)
        sssb_username = os.getenv("SSSB_USERNAME")
        sssb_pass = os.getenv("SSSB_PASS")

        # sssb_username = ''
        # sssb_pass = ''

        try:
            # Wait until the corresponding apartment link is loaded
            my_pages = WebDriverWait(self.browser, 15).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     """//*[@id="mina-sidor-trigger"]"""
                     )))
            my_pages.click()
            print("Clicked mypages link")

            username = self.browser.find_element_by_id("user_login")
            password = self.browser.find_element_by_id("user_pass")
            username.send_keys(sssb_username)
            password.send_keys(sssb_pass)
            login_attempt = self.browser.find_element_by_xpath("""//*[@id="header-loginform"]/button""")
            login_attempt.click()

            print("Clicked login link")

            self.logged_in = True

        except Exception as e:
            print("Could not login:\n\t" + str(e))

    def get_no_apartments(self):
        self.browser.get(SSSB_AVAILABLE_APARTMENTS)

        try:
            # Wait until the number of available apartments is displayed
            element = WebDriverWait(self.browser, 10).until(
                lambda wd: wd.find_element_by_xpath(
                    '//*[@id="SubNavigationContentContainer"]/strong/span').text != '0'
            )
            no_apts = int(
                self.browser.find_element_by_xpath("""//*[@id="SubNavigationContentContainer"]/strong/span""").text)

            return no_apts

        except TimeoutException:
            print("Loading number of apartments took too much time!")
            raise

    def get_apartment_and_offer(self, index):
        self.browser.get(SSSB_AVAILABLE_APARTMENTS)

        try:
            # Wait until the corresponding apartment link is loaded
            current_apt = WebDriverWait(self.browser, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     '//*[@id="SubNavigationContentContainer"]/div[4]/div[{0}]/div/div/div[2]/div/div[1]/h4/a'.format(
                         index)
                     )))
            current_apt.click()

            # Wait until the apartment info is loaded
            self.browser.implicitly_wait(2)
            try:
                apt_name = WebDriverWait(self.browser, 15).until(
                    EC.visibility_of_element_located(
                        (By.XPATH,
                         '//*[@id="SubNavigationContentContainer"]/div/div/div[1]/div[2]/h1'
                         )))

                apt_name = apt_name.text

                offering = WebDriverWait(self.browser, 15).until(
                    EC.visibility_of_element_located(
                        (By.XPATH,
                         '//*[@id="SubNavigationContentContainer"]/div/div/div[1]/div[6]/div'
                         )))
                offering = offering.text
                split_text = offering.split()
                end_date_and_time = '{0} {1}:00'.format(split_text[3], split_text[5])

                return [apt_name, end_date_and_time]

            except TimeoutException:
                print("Loading apartment took too much time!")
                return None

            except StaleElementReferenceException as e:
                print("Error getting element")
                return None

        except TimeoutException as e:
            print("Loading apartment link took too much time!")
            return None

    def scrape_offering(self):
        if self.logged_in:
            no_apts = self.get_no_apartments()

            db_conn_status = db_connection.is_connected()

            if not db_conn_status:
                db_connection.connect()

            try:
                # For each apartment
                i = 1
                # For avoiding dangerous loops
                j = 0
                while i <= no_apts:

                    if j >= 5:
                        raise ApartmentException("Cannot get past apartment \"{0}\"".format(apt_name))

                    info = self.get_apartment_and_offer(i)
                    if info is not None:
                        apt_name = info[0]
                        end_date_and_time = info[1]

                        try:
                            db_connection.set_is_offered(apt_name, end_date_and_time)
                            # Only advance to next apartment if the current one was successfully scraped.
                            i = i + 1
                            j = 0

                        except DatabaseException as e:
                            j = j + 1
                            print("Failure to insert some data: " + str(e))

            except DatabaseException as e:
                print(str(e))

            finally:
                if not db_conn_status:
                    db_connection.disconnect()

        else:
            # Apartments from current offering
            print("Cannot get offering. Not logged in.")

    def close_browser(self):
        self.browser.quit()


if __name__ == '__main__':
    sssb_selenium = SSSBApartmentOffer()
    sssb_selenium.login()
    sssb_selenium.scrape_offering()
    sssb_selenium.close_browser()
