#!/usr/bin/env python3

from urllib.parse import urlparse, urlunparse, urljoin
from anchorparser import AnchorParser
from multiprocessing import cpu_count

import importlib
import threading
import requests
import json
import os


# global configuration
try:
    with open('config.json') as config_file:
        config = json.load(config_file)
except FileNotFoundError as e:
    print(e)
    config = {}

thread_num = config.get('thread_num')
if not thread_num:
    thread_num = 2 * cpu_count
    
debug = config.get('debug')
if not debug:
    debug = True

database = config.get('database')
if not database:
    database = {}

if 'engine' not in database:
    if debug:
        database['engine'] = 'sqlite3'
    else:
        database['engine'] = 'psycopg2'

if 'info' not in database:
    database['info'] = {}

if database['engine'] == 'psycopg2':
    if 'user' not in database['info']:
        database['info']['user'] = 'preston'

    if 'database' not in database['info']:
        database['info']['database'] = 'crawler'

    specifier = '%s'
else:
    if 'filename' not in database['info']:
        database['info']['filename'] = 'crawler.db'

    specifier = '?'

sqlmodule = importlib.import_module(database['engine'])



def execute_script(cursor, script_file):
    if database['engine'] == 'sqlite3':
        cursor.executescript(script_file.read())
    else:
        cursor.execute(script_file.read())


def get_connection():
    """
    Return a connection object based on the global configuration of the module.
    """

    if 'filename' in database['info']:
        conn = sqlmodule.connect(database['info']['filename'])
    else:
        conn = sqlmodule.connect(**database['info'])
        conn.set_session(autocommit=True)

    return conn


def get_response(url):
    """
    Recursively follow redirection, until a valid page is found,
    and return the Response of that page.
    Assume url is valid.
    """

    response = requests.get(url)

    if not response.is_redirect:
        return response

    return requests.get(response.headers['location'])


def delete_url(url, cursor, conn = None):
    """
    Delete the specified url from the page table.
    """

    cursor.execute('delete from page where url = {}'.format(specifier), (url,))
    if conn:
        conn.commit()


def insert_pages(url, conn = None):
    """
    Update check date of url if valid; otherwise, delete url.
    Insert urls generated from anchor hrefs into database.
    """

    resp = get_response(url)
    is_not_html = not resp.headers['content-type'].startswith('text/html')

    close_conn = False
    if not conn:
        conn = sqlmodule.connect(**database['info'])
        close_conn = True

    cursor = conn.cursor()

    if url != resp.url:
        try:
            cursor.execute(
                'update page set url = {0} where url = {0}'.format(specifier),
                (resp.url, url)
            )
        except sqlmodule.IntegrityError as err:
            conn.rollback()
            delete_url(url, cursor, conn)
            cursor.close()
            if close_conn:
                conn.close()

            return
        else:
            conn.commit()
            url = resp.url
    elif resp.status_code != 200 or is_not_html:
        delete_url(url, cursor, conn)
        cursor.close()
        if close_conn:
            conn.close()

        return

    parser = AnchorParser()
    parser.feed(resp.text)

    process = lambda href: urlunparse(
        # remove fragments and query string
        urlparse(href)._replace(query='', fragment='')
    )

    # remove redundancy
    hrefs = set(list(map(process, parser.hrefs)))
    for href in hrefs:
        link_url = urljoin(url, href)
        scheme = urlparse(link_url).scheme
        bad_scheme = not scheme or (scheme != "http" and scheme != "https")
        if url == link_url or bad_scheme:
            continue
        try:
            cursor.execute(
                'insert into page(url) values({})'.format(specifier), (link_url,)
            )
        except sqlmodule.IntegrityError:
            conn.rollback()
        try:
            cursor.execute(
                'insert into anchor values({0}, {0})'.format(specifier),
                (url, link_url)
            )
        except sqlmodule.IntegrityError:
            conn.rollback()
            continue

        conn.commit()

    cursor.execute(
        'update page set last_check = now() where url = {}'.format(specifier),
        (url,)
    )
    print('"{}" has been resolved'.format(url))
    conn.commit()

    parser.close()
    cursor.close()

    if close_conn:
        conn.close()


def main():
    global thread_num, database
    conn = get_connection()
    cursor = conn.cursor()

    # conditional database init (see schema.sql)
    try:
        with open('schema.sql') as script_file:
            execute_script(cursor, script_file)
    except FileNotFoundError as e:
        print(e)
        exit(1)

    while True:
        threads = []
        cursor.execute(
            'select url from page where last_check is null limit {}'.format(
                specifier
            ),
            (thread_num,)
        )

        urls = cursor.fetchall()
        if not urls:
            break

        url = None
        for url in urls:
            URL_thread = threading.Thread(
                target=insert_pages, args=(url[0], conn)
            )
            print('Resolving "{}"...'.format(url[0]))
            URL_thread.start()
            threads.append(URL_thread)

        if not url:
            break

        for thread in threads:
            thread.join()

    conn.close()
    print('No URLs to crawl. Connection closed')


if __name__ == '__main__':
    main()
    exit(0)
    # success
