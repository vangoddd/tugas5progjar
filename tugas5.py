#!/usr/bin/env python3
# Foundations of Python Network Programming, Third Edition
# https://github.com/brandon-rhodes/fopnp/blob/m/py3/chapter08/queuepi.py
# Small application that uses several different message queues

import random
import threading
import time
import zmq

import sqlite3, os, sys
import hashlib

def sqlgenerator(zcontext, url):
    zsock = zcontext.socket(zmq.PUB)
    zsock.bind(url)

    count = 0
    total = 0
    value = 0

    db = sqlite3.connect("data.db")
    cur = db.cursor()

    time.sleep(5)

    for i in range(1000):
        n1 = random.randint(1, 99000)
        n2 = random.randint(1, 1000)
        sql = "select count(*) from MOCKDATA where ID>=%s AND ID<=%s;" % (n1, n1+n2)
        h = hash(sql)

        print(h)

        if h % 2 == 0:
            zsock.send_string("E," + sql)
            value = 0
        else:
            zsock.send_string("O," + sql)
            cur.execute(sql)
            value = int(cur.fetchone()[0])

        count += 1
        total += value
        #print("Gen", count)
        time.sleep(0.01)
    print("(gen) count: ", count, " Total: ", total)


def always_yes(zcontext, in_url, out_url):
    isock = zcontext.socket(zmq.SUB)
    isock.connect(in_url)
    isock.setsockopt(zmq.SUBSCRIBE, b"E")
    osock = zcontext.socket(zmq.PUSH)
    osock.connect(out_url)
    while True:
        val = isock.recv_string()
        msg = val.split(',')[1] + " count() = 0"
        osock.send_string(msg)


def requestservice(zcontext, in_url, sqlitegenerator_url, out_url):
    isock = zcontext.socket(zmq.SUB)
    isock.connect(in_url)
    isock.setsockopt(zmq.SUBSCRIBE, b'O')
    psock = zcontext.socket(zmq.REQ)
    psock.connect(sqlitegenerator_url)
    osock = zcontext.socket(zmq.PUSH)
    osock.connect(out_url)
    while True:
        # receive from generator
        val = isock.recv_string()
        # send to SqLite
        sql = val.split(',')[1]
        psock.send_string(sql)
        # receive from sqlite
        resp = psock.recv_string()
        # send to print
        osock.send_string(sql + resp)


def sqlitegenerator(zcontext, url):
    db = sqlite3.connect("data.db")
    cur = db.cursor()

    zsock = zcontext.socket(zmq.REP)
    zsock.bind(url)
    while True:
        sql = zsock.recv_string()
        cur.execute(sql)
        value = int(cur.fetchone()[0])
        responseString = " count() = " + str(value);
        zsock.send_string(responseString)


def printoutput(zcontext, url):
    count = 0
    total = 0

    zsock = zcontext.socket(zmq.PULL)
    zsock.bind(url)
    while True:
        msg = zsock.recv_string()
        count += 1

        queryCount = msg.split()
        total += int(queryCount[-1])

        print(msg)
        #print(count)
        if count >= 1000:
            print("count: ", count, " Total: ", total)
            break;


def start_thread(function, *args):
    thread = threading.Thread(target=function, args=args)
    thread.daemon = True  # so you can easily Ctrl-C the whole program
    thread.start()


def main(zcontext):

    pubsub = 'tcp://127.0.0.1:6700'
    reqrep = 'tcp://127.0.0.1:6701'
    pushpull = 'tcp://127.0.0.1:6702'
    
    start_thread(always_yes, zcontext, pubsub, pushpull)
    start_thread(requestservice, zcontext, pubsub, reqrep, pushpull)
    start_thread(sqlitegenerator, zcontext, reqrep)
    start_thread(printoutput, zcontext, pushpull)
    start_thread(sqlgenerator, zcontext, pubsub)
    time.sleep(60)


if __name__ == '__main__':
    main(zmq.Context())
