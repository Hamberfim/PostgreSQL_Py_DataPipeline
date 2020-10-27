# -*- coding: utf-8 -*-
"""
Program: ingest_data.py
Created on 10/16/2020
@author: adhamlin


This program ingest data from 'pseudo_api.py' file. 'pseudo_api.py' must be running.
"""
# imports
import requests
import psycopg2


""" TEST
with requests.get("http://127.0.0.1:5000/large_data_request/10") as r:
    print(r.text)
"""

# get N rows of transactional data
with requests.get("http://127.0.0.1:5000/large_data_request/10", stream=True) as r:  # process line by line

    conn = psycopg2.connect(dbname="pseudo_data_stream_test",
                            user="postgres",
                            password="[password]")  # changed on commit to github
    cur = conn.cursor()
    sql = "INSERT INTO transactions (txid, uid, amount) VALUES (%s, %s, %s)"

    buffer = ""
    for chunk in r.iter_content(chunk_size=1):
        if chunk.endswith(b'\n'):  # '\n' used as end of line indicator in 'pseudo_api.py'
            t = eval(buffer)
            print(t)  # tuple from pseudo_api.py'
            if t[2] > 1000 or t[2] < -1000:  # simulate flag on transaction
                print("^^^^^^^^ LARGE TRANSACTION ^^^^^^^^")  #
            cur.execute(sql, (t[0], t[1], t[2]))
            conn.commit()
            buffer = ""
        else:
            buffer += chunk.decode()
