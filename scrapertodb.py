#!/usr/bin/python
# Python 2.7 scraper for https://mentor.ieee.org/802.11/documents
# TODO: Reimplement from BeautifulSoup to lxml because they say lxml is faster.

import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error

def create_server_connection(host_name, user_name, user_password):
    # TODO: don't have DB connection info hard-coded
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            auth_plugin='mysql_native_password',
            database='ieee80311'
        )
        print("MySQL Database connection successful")
    except Error as err:
        print("Hit exception, no MySQL connection made: %s" % err)
    return connection

def parse_datarow(datarow):
    # datarow is a bs4.element.Tag
    all_fields = datarow.find_all('td')
    argument_array = []
    try:
        # This needs to match up to the SQL INSERT statement:
        argument_array.append(all_fields[0].contents[0].contents[0].encode('utf-8')[:50])
        argument_array.append(all_fields[1].contents[0].encode('utf-8'))
        argument_array.append(all_fields[2].contents[0].encode('utf-8'))
        argument_array.append(all_fields[3].contents[0].encode('utf-8'))
        argument_array.append(all_fields[4].contents[0].encode('utf-8')[:200])
        argument_array.append(all_fields[5].contents[0].encode('utf-8')[:200])
        argument_array.append(all_fields[6].contents[0].encode('utf-8')[:45])
        argument_array.append(all_fields[7].contents[0].contents[0].encode('utf-8')[:45])
        argument_array.append(all_fields[8].contents[0]['href'].encode('utf-8')[:300])
    except Error as err:
        print ("Exception parsing: %s" % err)
        return None
    return argument_array

# TODO: Get the password from an environmental variable instead.
conn = create_server_connection('localhost','ieeeuser','Ca5I3XFY')
curs = conn.cursor()

LAST_PAGE = 744 
for pagenum in range(1, LAST_PAGE + 1):
    args = {'n': '%d' % pagenum}
    page = requests.get("https://mentor.ieee.org/802.11/documents", params=args)
    print page.url
    contents = page.content
    soup = BeautifulSoup(contents, 'html.parser')
    args = { "class" : "b_data_row"}
    for row in soup.find_all('tr', attrs = args ):
        argument_array = parse_datarow(row)
        curs.execute("INSERT INTO weetabix (CREATED_DATE, DCN_YEAR, DCN_DCN, \
        DCN_REV, GROOP, TITLE, AUTH_AFFIL, UPLOADED_DATE, FILENAME) \
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", argument_array)
        conn.commit()
if conn:
    conn.close()

