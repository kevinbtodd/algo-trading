import requests
import time
import threading
import queue
import csv
import sqlite3

import bitfinex
import kraken

""" 
    - This script collects standardised bitcoin tick data from any major exchange using their API's. 
    - It checks that the timestamps on each tick are matching before storing them in a database. 
    - It is important that the timestamps match when being used in live trading as if they do not match, 
      a false trading signal may be generated (you dont want to input a 10 minute old price with a 2 
      second old price into a trading algorithm).
    - The script uses multithreading to simultaneously send requests to any number of exchanges.
    - The ticks are stored in a database in a standardised format:
        ID - Unique tick identifier
        BID - Current exchange bid price
        ASK - Current exchange ask price
        TIMESTAMP - UNIX timestamp when the price information was valid
"""

# MAKE SURE TABLE NAMES ARE IN THE RIGHT ORDER (LOWEST ID NUMBER FIRST)
tables = ['bitfinex', 'kraken'] # Name of the tables in the database
database = 'ticks.db' # Name of the database

num_exchanges = len(tables) # number of exchanges
get_q = queue.Queue(maxsize = num_exchanges)

margin = 3 # acceptable difference between timestamps

num_threads = num_exchanges + 1 # number of exchanges + 1
b1 = threading.Barrier(num_threads)

b2 = threading.Barrier(2) # always 2

sorted_q = queue.Queue(maxsize = 1) # always 1

def requestTicks(exchange, identifier): # n threads = 3

    while True:

        time.sleep(2)

        response = exchange.get_tick()

        get_q.put(response)

        print(identifier + ' request ok')

        # wait for all responses
        b1.wait()

def sortTicks(): # n threads = 1

    global margin 
    
    while True:

        # wait for all responses to be added to queue before getting responses from the queue
        b1.wait()

        unsorted_ticks = []
        global num_exchanges

        for i in range(num_exchanges):
            unsorted_ticks.append(get_q.get())

        get_q.task_done()

        # sort ticks in order of id
        ticks = sorted(unsorted_ticks, key=lambda k: k['id'])

        # check timestamps are within +- t seconds of eachother
        base = ticks[0]
        truthcounter = 0
        basetime = base['timestamp']

        for i in ticks:

            timestamp = i['timestamp']

            if basetime - margin <= timestamp <= basetime + margin:
                truthcounter += 1

        if truthcounter == len(ticks):
            sync_ticks = ticks

            # put list of synchronised ticks into queue
            sorted_q.put(sync_ticks)

            print('sync success')

            # wait for sorted ticks to get put in queue
            b2.wait()

        # if ticks not synchronised, get new ticks
        else:
            continue

# store ticks in SQLite database, takes list of table names as argument
def storeTicks(tables, database):

    # connect to sqlite database
    conn = sqlite3.connect(database)
    c = conn.cursor()

    size = len(tables)

    # iterate through each table in tables, creating an sqlite table each time
    for i in range(size):
        table = tables[i]

        # create table 'ticks'
        c.execute("""CREATE TABLE IF NOT EXISTS """ + table + """ (
                    id integer primary key,
                    bid real,
                    ask real,
                    timestamp real
                    )""")

    conn.commit()
    conn.close()

    # write tick data
    while True:

        # connect to sqlite database
        conn = sqlite3.connect(database)
        c = conn.cursor()

        # wait for sorted ticks to get put in queue
        b2.wait()

        # get the synchronised ticks from the sorted queue
        sync_ticks = sorted_q.get()

        sorted_q.task_done()

        j = 0

        for i in sync_ticks:
            bid = i['bid']
            ask = i['ask']
            timestamp = i['timestamp']

            table = tables[j]

            # store tick in SQL database
            c.execute("INSERT INTO " + table + " (bid, ask, timestamp) VALUES (?, ?, ?)", (bid, ask, timestamp))

            print('insert success')

            j += 1

        # close connection
        conn.commit()
        conn.close()

# start all threads here
#t1 = threading.Thread(target = requestTicks, args = (cex, 'cex'))
t2 = threading.Thread(target = requestTicks, args = (bitfinex, 'bfx'))
t3 = threading.Thread(target = requestTicks, args = (kraken, 'krk'))
t4 = threading.Thread(target = sortTicks, args = ())

#t1.daemon = True
t2.daemon = True
t3.daemon = True
t4.daemon = True

#t1.start()
t2.start()
t3.start()
t4.start()

storeTicks(tables, database)
