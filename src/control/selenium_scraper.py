from selenium import webdriver
import time
from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

sssb_username = os.getenv("SSSB_USERNAME")
sssb_pass = os.getenv("SSSB_PASS")

no_apts = 2

dirname = os.path.dirname(__file__)

browser = webdriver.Chrome(executable_path=os.path.join(dirname, '..\\..\\Dependencies\\Selenium\\chromedriver.exe'))
browser.set_page_load_timeout(10)
browser.get("https://www.sssb.se/en")
browser.implicitly_wait(10)
# time.sleep(10)

# Logging in
my_pages = browser.find_element_by_xpath("""//*[@id="mina-sidor-trigger"]""")
my_pages.click()
username = browser.find_element_by_id("user_login")
password = browser.find_element_by_id("user_pass")
username.send_keys(sssb_username)
password.send_keys(sssb_pass)
login_attempt = browser.find_element_by_xpath("""//*[@id="header-loginform"]/button""")
login_attempt.click()

end_date_and_time = []
# Apartments from current offering
browser.get("https://www.sssb.se/en/find-apartment/apply-for-apartment/available-apartments/?paginationantal=50")

for i in range(1, no_apts + 1):
    browser.implicitly_wait(10)
    current_apt = browser.find_element_by_xpath("""//*[@id="SubNavigationContentContainer"]/div[4]/div[{0}]/div/div/div[2]/div/div[1]/h4/a""".format(i))
    current_apt.click()
    browser.implicitly_wait(10)
    apt_name = browser.find_element_by_xpath("""//*[@id="SubNavigationContentContainer"]/div/div/div[1]/div[2]/h1""").text
    offering = browser.find_element_by_xpath("""//*[@id="SubNavigationContentContainer"]/div/div/div[1]/div[6]/div""").text
    split_text = offering.split()
    end_date_and_time.append([split_text[3], split_text[5]])

    print apt_name, offering

    browser.get("https://www.sssb.se/en/find-apartment/apply-for-apartment/available-apartments/?paginationantal=50")

browser.implicitly_wait(15)
browser.quit()

# print end_date_and_time
