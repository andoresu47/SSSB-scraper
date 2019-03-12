"""Module for managing connections to sssb_data database, as well as manipulation of data.

"""

__author__ = 'Andres'

import psycopg2
import logging
import os
import subprocess
import pandas as pd
from dotenv import load_dotenv

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


def set_apartment(apt_name, apt_type, apt_zone, apt_price, furnitured='False', electricity='False', _10_month='False'):
    """Function for inserting a new valid row into the "apartment" table.
        In case of success the corresponding id is returned.

    Args:
        apt_name: string representing the address of an apartment.
        apt_type:
        apt_zone:
        apt_price:
        furnitured:
        electricity:
        _10_month:

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
        ticker_id = int(cur.fetchone()[0])
        log.info('Apartment: Committing transaction')
        conn.commit()
        cur.close()
        return ticker_id
    except Exception as e:
        conn.rollback()
        log.error('Apartment: Rolling back transaction')
        log.exception("Apartment: Couldn't insert successfully")
        raise


if __name__ == '__main__':
    connect()

    apt_name = 'Körsbärsvägen 4 C / 1110'
    apt_type = 'Enkelrum, (rum i korridor)'
    apt_zone = 'Forum'
    apt_price = 3799

    set_apartment(apt_name, apt_type, apt_zone, apt_price)

    disconnect()
