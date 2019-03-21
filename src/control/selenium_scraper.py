"""Module defining the Selenium crawler that retrieves logged-in data from SSSB.

"""

__author__ = 'Andres'

from selenium import webdriver
from dotenv import load_dotenv
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import src.data.db_ops as db_connection
from src.data.db_ops import DatabaseException

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)
sssb_username = os.getenv("SSSB_USERNAME")
sssb_pass = os.getenv("SSSB_PASS")

# sssb_username = ''
# sssb_pass = ''

dirname = os.path.dirname(__file__)

options = webdriver.FirefoxOptions()
options.add_argument('-headless')

browser = webdriver.Firefox(executable_path=os.path.join(dirname, '../../Dependencies/Selenium/geckodriver'))
browser.set_page_load_timeout(30)
browser.get("https://www.sssb.se/en")

# Logging in
try:
    # Wait until the corresponding apartment link is loaded
    my_pages = WebDriverWait(browser, 15).until(
        EC.presence_of_element_located(
            (By.XPATH,
             """//*[@id="mina-sidor-trigger"]"""
             )))
    my_pages.click()
    print("Clicked mypages link")

    username = browser.find_element_by_id("user_login")
    password = browser.find_element_by_id("user_pass")
    username.send_keys(sssb_username)
    password.send_keys(sssb_pass)
    login_attempt = browser.find_element_by_xpath("""//*[@id="header-loginform"]/button""")
    login_attempt.click()

    print("Clicked login link")

    # Apartments from current offering
    browser.get("https://www.sssb.se/en/find-apartment/apply-for-apartment/available-apartments/?paginationantal=all")

    db_conn_status = db_connection.is_connected()

    if not db_conn_status:
        db_connection.connect()

    try:
        try:
            # Wait until the number of available apartments is displayed
            element = WebDriverWait(browser, 10).until(
                lambda wd: wd.find_element_by_xpath('//*[@id="SubNavigationContentContainer"]/strong/span').text != '0'
            )
            no_apts = int(
                browser.find_element_by_xpath("""//*[@id="SubNavigationContentContainer"]/strong/span""").text)

        finally:
            # For each apartment
            i = 1
            while i <= no_apts:
            # for i in range(1, no_apts + 1):
                try:
                    # Wait until the corresponding apartment link is loaded
                    current_apt = WebDriverWait(browser, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                             '//*[@id="SubNavigationContentContainer"]/div[4]/div[{0}]/div/div/div[2]/div/div[1]/h4/a'.format(
                                 i)
                             )))
                    current_apt.click()

                    # Wait until the apartment info is loaded
                    try:
                        apt_name = WebDriverWait(browser, 10).until(
                            EC.presence_of_element_located(
                                (By.XPATH,
                                 '//*[@id="SubNavigationContentContainer"]/div/div/div[1]/div[2]/h1'
                                 )))

                        apt_name = apt_name.text

                        offering = WebDriverWait(browser, 10).until(
                            EC.presence_of_element_located(
                                (By.XPATH,
                                 '//*[@id="SubNavigationContentContainer"]/div/div/div[1]/div[6]/div'
                                 )))
                        offering = offering.text
                        split_text = offering.split()
                        end_date_and_time = '{0} {1}:00'.format(split_text[3], split_text[5])

                        try:
                            db_connection.set_is_offered(apt_name, end_date_and_time)
                            # Only advance to next apartment if the current one was successfully scraped.
                            i = i + 1

                        except DatabaseException as e:
                            print("Failure to insert some data: " + str(e))

                    except TimeoutException:
                        print("Loading apartment took too much time!")

                    finally:
                        # Always go back to the apartment selection web page
                        browser.get(
                            "https://www.sssb.se/en/find-apartment/apply-for-apartment/available-apartments/?paginationantal=all")

                except TimeoutException:
                    print("Loading apartment link took too much time!")
                    # If there is a timeout exception, just reload
                    browser.refresh()

    except DatabaseException as e:
        print(str(e))

    finally:
        if not db_conn_status:
            db_connection.disconnect()

except Exception as e:
    print("Something went wrong:\n\t" + str(e))

finally:
    # browser.implicitly_wait(10)
    browser.quit()
