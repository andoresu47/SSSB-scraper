# coding=utf-8
"""Module for managing connections to sssb_data database, as well as manipulation of data.

"""

__author__ = 'Andres'

import psycopg2
import logging
import os
import subprocess
import pandas as pd
from dotenv import load_dotenv
import time

# Initialization of global db connection and logging config.

conn = None

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_host = os.getenv("DB_HOST")
db_pass = os.getenv("DB_PASS")

dirname = os.path.dirname(__file__)
hdlr = logging.FileHandler(os.path.join(dirname,
                                        '..\\..\\Logs\\SSSBData.log'),
                           encoding="UTF-8")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)

log = logging.getLogger('sssb_data')
log.addHandler(hdlr)
log.addHandler(consoleHandler)
log.setLevel(logging.INFO)


class DatabaseException(Exception):
    """Class for managing database exceptions.

    """

    pass


def is_connected():
    """Function to determine if there is currently an active connection to db.

    Raises:
        DatabaseException: If something impedes to connect to the database.

    """

    global conn

    return True if conn is not None else False


def connect():
    """Function in charge of connecting to the database "sssb_data".

    Raises:
        DatabaseException: If something impedes to connect to the database.

    """

    global conn, log

    try:
        conn = psycopg2.connect("dbname={0} user={1} host={2} password={3}".format(db_name,
                                                                                   db_user,
                                                                                   db_host,
                                                                                   db_pass))
        conn.set_client_encoding("utf-8")

    except Exception as e:
        log.exception("Failed to connect")
        raise DatabaseException("Failed to connect\n" + str(e))


def disconnect():
    """Function in charge of disconnecting from database.

    Raises:
        DatabaseException: If something impedes to disconnect from database.

    """

    global conn, log

    try:
        conn.close()
        conn = None
    except Exception as e:
        log.exception("Unable to disconnect from database")
        raise DatabaseException("Unable to disconnect from database\n" + str(e))


def get_timestamp():
    """Function to get today's date.

    Returns:
         string: today's date in the format YYYYMMDD.

    """

    return time.strftime('%Y-%m-%d %H:%M:%S')


def set_apartment(apt_name, apt_type, apt_zone, apt_price, furnitured='False', electricity='False', _10_month='False'):
    """Function for inserting a new valid row into the "apartment" table.
        In case of success the corresponding id is returned.

    Args:
        apt_name: string representing the address of an apartment.
        apt_type: string representing an apartment type. E.g.: single room, room with kitchenette, etc.
        apt_zone: string representing the general location of an apartment.
        apt_price: monthly rent.
        furnitured: true if furnitured, false otherwise.
        electricity: true if electricity is included, false otherwise.
        _10_month: true if the contract is for 10 months only, false otherwise.

    Raises:
        Exception: In case no insertion was possible.

    Returns:
        int: integer representing the inserted row ID.

    """

    global conn, log

    cur = conn.cursor()
    try:
        log.info('Apartment: Inserting new apartment: {0}'.format(apt_name))
        sql = """INSERT INTO apartment (name, type, zone, price, furnitured, electricity, _10_month) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s) 
                    RETURNING nIdapartment"""
        cur.execute(sql, (apt_name, apt_type, apt_zone, apt_price, furnitured, electricity, _10_month))
        apartment_id = int(cur.fetchone()[0])
        log.info('Apartment: Committing transaction')
        conn.commit()
        cur.close()
        return apartment_id
    except Exception as e:
        conn.rollback()
        log.error('Apartment: Rolling back transaction')
        log.exception("Apartment: Couldn't insert successfully")
        raise DatabaseException(str(e))


def set_offer(end_date, start_date=get_timestamp()):
    """Function for inserting a new valid row into the "offer" table.
        In case of success the corresponding id is returned.

    Args:
        start_date: string representing the starting date and time of an offer.
        end_date: string representing the ending date and time of an offer.

    Raises:
        Exception: In case no insertion was possible.

    Returns:
        int: integer representing the inserted row ID.

    """

    global conn, log

    cur = conn.cursor()
    try:
        log.info('Offer: Inserting new offer: {0}'.format(end_date))
        sql = """INSERT INTO offer (start_date, end_date) 
                    VALUES (%s, %s) 
                    RETURNING nIdOffer"""
        cur.execute(sql, (start_date, end_date))
        offer_id = int(cur.fetchone()[0])
        log.info('Offer: Committing transaction')
        conn.commit()
        cur.close()
        return offer_id
    except Exception as e:
        conn.rollback()
        log.error('Offer: Rolling back transaction')
        log.exception("Offer: Couldn't insert successfully")
        print("Couldn't insert offer: " + str(e))
        raise DatabaseException(str(e))


def set_is_offered(apt_address, time_stamp):
    """Function for inserting a new valid row into the "is_offered" table.

    Args:
        apt_address: string representing an apartment's address.
        time_stamp: string representing a timestamp in which the apartment is being offered.

    Raises:
        Exception: In case no insertion was possible.

    """

    global conn, log

    cur = conn.cursor()
    try:
        log.info('Apartment-Offer: Inserting new relationship: {0}-{1}'.format(apt_address, time_stamp))
        sql = """INSERT INTO isOffered (nIdApartment, nIdOffer) 
                    VALUES (%s, %s)"""
        apt_id = get_apartment_id(apt_address)
        offer_id = get_offer_id(time_stamp)
        if offer_id is None:
            offer_id = set_offer(time_stamp)

        cur.execute(sql, (apt_id, offer_id))
        log.info('Apartment-Offer: Committing transaction')
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        log.error('Apartment-Offer: Rolling back transaction')
        log.exception("Apartment-Offer: Couldn't insert successfully")
        print("Couldn't insert apartment-offer relation: " + str(e))
        raise DatabaseException(str(e))


def set_apartment_state(state_timestamp, apt_address, no_applicants, top_credits):
    """Function in charge of inserting valid new rows into apartment State table.

    Args:
        state_timestamp: timestamp in which the data was retrieved.
        apt_address: address representing an apartment.
        no_applicants: number of applicants up to the corresponding timestamp.
        top_credits: maximum credits from among applicants.

    Raises:
        DatabaseException: If something impedes to insert new data, such as repeated entries.

    """

    global conn, log

    cur = conn.cursor()
    try:
        log.info('Apartment State: Inserting state for: {0}'.format(apt_address))
        sql = """INSERT INTO State (time_stamp, nIdApartment, nIdOffer, no_applicants, top_credits) 
                    VALUES (%s, %s, %s, %s, %s)"""
        offer = get_offer_id(state_timestamp)
        cur.execute(sql, (state_timestamp, get_apartment_id(apt_address), offer, no_applicants, top_credits))

        log.info('Apartment State: Committing transaction')
        conn.commit()
        cur.close()

    except Exception as e:
        conn.rollback()
        log.error('Apartment State: Rolling back transaction')
        log.exception("Apartment State: Couldn't insert successfully")
        raise DatabaseException(str(e))


def get_apartment_id(address):
    """Function for retrieving the id of a certain apartment.

    Args:
        address: string representing an apartment.

    Returns:
        int: integer representing the desired ID.

    """

    global conn, log

    cur = conn.cursor()
    try:
        log.info('Apartment (get): Querying for: {0}'.format(address))
        sql = """SELECT nIdapartment 
                  FROM apartment
                  WHERE name = %s"""
        cur.execute(sql, (address,))
        res = cur.fetchone()
        log.info('Apartment (get): Committing transaction')
        conn.commit()
        cur.close()
        if res is not None:
            return int(res[0])
        else:
            conn.rollback()
            log.error('Apartment (get): Rolling back transaction')
            log.exception("Apartment (get): No matching record found")
            print("No matching record found")

    except Exception as e:
        conn.rollback()
        log.error('Apartment (get): Rolling back transaction')
        log.exception("Apartment (get): Couldn't retrieve apartment id")
        DatabaseException(str(e))


def get_offer_id(time_stamp):
    """Function for retrieving the id of a certain apartment offering.

    Args:
        time_stamp: string representing an timestamp.

    Returns:
        int: integer representing the desired ID.

    """

    global conn, log

    cur = conn.cursor()
    try:
        log.info('Offer (get): Querying for: {0}'.format(time_stamp))
        sql = """SELECT nIdOffer 
                  FROM Offer
                  WHERE start_date <= %s AND end_date >= %s"""
        cur.execute(sql, (time_stamp, time_stamp))
        res = cur.fetchone()
        log.info('Offer (get): Committing transaction')
        conn.commit()
        cur.close()
        if res is not None:
            return res[0]
        else:
            return None

    except Exception as e:
        conn.rollback()
        log.error('Offer (get): Rolling back transaction')
        log.exception("Offer (get): Couldn't retrieve offer id")
        DatabaseException(str(e))


def get_current_offer_dates():
    """Function for retrieving the start and end dates of the current apartment offering.

    Returns:
        (datetime.datetime, datetime.datetime): tuple representing the desired timestamps.

    """

    global conn, log

    cur = conn.cursor()
    try:
        log.info('Offer (get): Querying for last offer')
        sql = """SELECT start_date, end_date from offer 
                    GROUP BY nidoffer 
                    HAVING nidoffer = max(nidoffer)"""
        cur.execute(sql)
        res = cur.fetchall()
        log.info('Offer (get): Committing transaction')
        conn.commit()
        cur.close()
        if res is not None:
            return res[0]
        else:
            return None

    except Exception as e:
        conn.rollback()
        log.error('Offer (get): Rolling back transaction')
        log.exception("Offer (get): Couldn't retrieve dates")
        DatabaseException(str(e))


# if __name__ == '__main__':
#     connect()
#
#     apt_name = 'Körsbärsvägen 4 C / 1110'
#     apt_type = 'Enkelrum, (rum i korridor)'
#     apt_zone = 'Forum'
#     apt_price = 3799
#
#     set_apartment(apt_name, apt_type, apt_zone, apt_price)
#     print(get_apartment_id(apt_name))
#     offer = set_offer(get_timestamp(), '2019-03-31 22:40:01')
#     offer = get_offer_id('2019-03-21 12:17:04')
#     print(offer)
#
#     disconnect()
