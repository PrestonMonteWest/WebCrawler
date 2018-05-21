#!/usr/bin/env python3

from urllib.parse import urlparse, urlunparse, urljoin
from html.parser import HTMLParser

import psycopg2
import requests
import threading
import json
import os


class MyHTMLParser(HTMLParser):
    def __init__(self, *, convert_charrefs=True):
        HTMLParser.__init__(self)
        self.hrefs = []
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr in attrs:
                if attr[0] == "href":
                    self.hrefs.append(attr[1])
                    break

def get_response(url):
    """
    Recursively follow redirection, until a valid page is found,
    and return the Response of that page.
    Assume url is valid.
    """

    response = requests.get(url)

    if not response.is_redirect:
        return response

    return requests.get(response.headers["location"])

def delete_url(url, cursor, conn):
    """
    Delete the specified url from the page table.
    """

    cursor.execute("delete from page where url = %s", (url,))
    conn.commit()

def insert_pages(url, conn = None):
    """
    Update check date of url if valid; otherwise, delete url.
    Insert urls generated from anchor hrefs into database.
    """

    resp = get_response(url)
    is_not_html = not resp.headers["content-type"].startswith("text/html")

    close_conn = False
    if not conn:
        conn = connect()
        close_conn = True

    cursor = conn.cursor()

    if url != resp.url:
        try:
            cursor.execute(
                "update page set url = %s where url = %s", (resp.url, url)
            )
        except pgdb.IntegrityError as err:
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
    
    parser = MyHTMLParser()
    parser.feed(resp.text)
    
    process = lambda href: urlunparse(
        # remove fragments and query string
        urlparse(href)._replace(query="", fragment="")
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
            cursor.execute("insert into page(url) values(%s)", (link_url,))
        except pgdb.IntegrityError:
            conn.rollback()
        try:
            cursor.execute("insert into link values(%s, %s)", (url, link_url))
        except pgdb.IntegrityError:
            conn.rollback()
            continue
        
        conn.commit()
        
    cursor.execute("update page set last_check = now() where url = %s", (url,))
    print("'{}' has been resolved.".format(url))
    conn.commit()

    parser.close()
    cursor.close()

    if close_conn:
        conn.close()


def main():
    with open("config.json") as config_file:
        config = json.load(config_file)

    conn = psycopg2.connect(**config['database_info'])
    thread_num = config['thread_num']

    while True:
        threads = []
        urls = conn.cursor().execute(
            "select url from page where last_check is null limit %s",
            (thread_num,)
        )

        url = None
        for url in urls:
            myThread = threading.Thread(
                target=insert_pages, args=(url[0],)
            )
            print("Resolving '{}'...".format(url[0]))
            myThread.start()
            threads.append(myThread)

        if not url:
            break

        for thread in threads:
            thread.join()

    urls.close()
    conn.close()


if __name__ == "__main__":
    main()
