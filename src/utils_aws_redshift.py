# coding: utf-8
import psycopg2
from psycopg2.extras import wait_select

from configparser import SafeConfigParser
parser = SafeConfigParser()


def open_connection(dbname, user, host, port, password):
    connect_text = "dbname='{}' user='{}' host={} port={} password='{}'".format(
        dbname, user, host, port, password)
    connector = psycopg2.connect(connect_text)
    cursor = connector.cursor()
    return connector, cursor


def open_connection_async(dbname, user, host, port, password):
    connect_text = "dbname='{}' user='{}' host={} port={} password='{}'".format(
        dbname, user, host, port, password)
    connector = psycopg2.connect(connect_text, async=1)
    # wait(connector)
    wait_select(connector)
    cursor = connector.cursor()
    return connector, cursor


def close_connection(connector, cursor):
    cursor.close()
    connector.close()
    return None


#%%
def run_query_py(query: str, connector, cursor, prompt: bool=False, asynchronous=False):
    # http://initd.org/psycopg/docs/usage.html
    try:
        cursor.execute(query)
        if prompt:
            for record in cursor:
                print(record)

        connector.commit()
    except Exception as e:
        if not asynchronous:
            connector.rollback()
        raise e
        
def run_query_py_async(query: str, connector, cursor, prompt: bool=False):
    # http://initd.org/psycopg/docs/usage.html
    try:
        cursor.execute(query)
        wait_select(cursor.connection)
        connector.commit()
    except Exception as e:
        raise e        
