# coding: utf-8

import datetime
import mysql.connector

conn = mysql.connector.connect(
                                user='nagaryu000',
                                password='ryujinagasawa',
                                database='mysql_connector_python',
                                host='127.0.0.1')

cursor = conn.cursor()

query = ("SELECT first_name, last_name, hire_date FROM employees "
        "where hire_date BETWEEN %s AND %s")

hire_start = datetime.date(2019, 1, 1)
hire_end = datetime.date(2020, 12, 31)

cursor.execute(query, (hire_start, hire_end))

for first_name, last_name, hire_date in cursor:
    print(f"{last_name} {first_name} was hired on {hire_date:%Y-%m-%d}.")

cursor.close()
conn.close()