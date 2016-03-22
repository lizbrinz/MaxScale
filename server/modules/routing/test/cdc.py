#!/usr/bin/env python

import sys
import json
import socket
import hashlib

sock = socket.create_connection(["127.0.0.1", "4001"])
auth_string = "massi:".encode('hex') + hashlib.sha1("massi").hexdigest()

sock.send(auth_string)
response = sock.recv(1024).encode('utf_8')

sock.send("REGISTER UUID=XXX-YYY_YYY, TYPE=AVRO")
response = sock.recv(1024).encode('utf_8')

sock.send("REQUEST-DATA " + sys.argv[1])

decoder = json.JSONDecoder()
objlist = []

rbuf = str(sock.recv(1024))
words = dict()
wordcount = 0

while True:
    try:
        data = decoder.raw_decode(rbuf)
        rbuf = rbuf[data[1]:]
        print(json.dumps(data[0]))
    except Exception as ex:
        rbuf += str(sock.recv(1024))
