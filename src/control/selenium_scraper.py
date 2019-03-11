from selenium import webdriver
import time
from dotenv import load_dotenv
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import _find_element
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

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
    print "Clicked mypages link"

    username = browser.find_element_by_id("user_login")
    password = browser.find_element_by_id("user_pass")
    username.send_keys(sssb_username)
    password.send_keys(sssb_pass)
    login_attempt = browser.find_element_by_xpath("""//*[@id="header-loginform"]/button""")
    login_attempt.click()
    
    print "Clicked login link"

    end_dates_and_times = []

    # Apartments from current offering
    browser.get("https://www.sssb.se/en/find-apartment/apply-for-apartment/available-apartments/?paginationantal=all")

    try:
        # Wait until the number of available appartments is displayed
        element = WebDriverWait(browser, 10).until(
            lambda wd: wd.find_element_by_xpath('//*[@id="SubNavigationContentContainer"]/strong/span').text != '0'
        )
        no_apts = int(browser.find_element_by_xpath("""//*[@id="SubNavigationContentContainer"]/strong/span""").text)

    finally:
        # For each apartment
        for i in range(1, no_apts + 1):
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
                    offering = browser.find_element_by_xpath(
                       """//*[@id="SubNavigationContentContainer"]/div/div/div[1]/div[6]/div""").text
                    split_text = offering.split()
                    end_date_and_time = [split_text[3], split_text[5]]
                    end_dates_and_times.append(end_date_and_time)

                    print apt_name, end_date_and_time

                except TimeoutException:
                    print "Loading apartment took too much time!"

                finally:
                    # Always go back to the apartment selection web page
                    browser.get(
                        "https://www.sssb.se/en/find-apartment/apply-for-apartment/available-apartments/?paginationantal=all")

            except TimeoutException:
                print "Loading appartment link took too much time!"
                # If there is a timeout exception, just reload
                browser.refresh()

except Exception as e:
    print "Something went wrong when logging in..."

finally:
    # browser.implicitly_wait(10)
    browser.quit()
