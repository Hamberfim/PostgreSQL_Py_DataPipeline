# -*- coding: utf-8 -*-
"""
Program: pseudo_api.py
Created on 10/16/2020
@author: adhamlin

This program mocks up and returns pseudo transactional data.
"""
# imports
from flask import Flask, Response, stream_with_context
import time
import uuid
import random

APP = Flask(__name__)


# once ran go to http://127.0.0.1:5000/large_data_request/ adding the number of rows the generator should return
# i.e., http://127.0.0.1:5000/large_data_request/100 to return 100 rows
@APP.route("/large_data_request/<int:rowcount>", methods=["GET"])  # 127.0.0.1:5000/large_data_request/<int:rowcount>
def get_large_request(rowcount):
    """return N rows of data"""
    def f():
        """The generator for mocking transactional data"""
        for _i in range(rowcount):
            time.sleep(.01)  # faster API return time.sleep(.001)
            txid = uuid.uuid4()  # generate universally unique identifiers as transaction id
            uid = uuid.uuid4()  # generate universally unique identifiers as user id
            amount = round(random.uniform(-1100, 1100), 2)  # dollar amounts form -1000 to 1000
            print(txid, amount)  # out to console
            # txid = transaction id, uid = user id, amount = transaction amount
            yield f"('{txid}', '{uid}', {amount})\n"  # '\n' used as end of line indicator
    return Response(stream_with_context(f()))


# main/driver if needed
if __name__ == '__main__':
    # once ran go to http://127.0.0.1:5000/large_data_request/ adding the number of rows the generator should return
    # i.e., http://127.0.0.1:5000/large_data_request/100 to return 100 rows
    APP.run(debug=True)
