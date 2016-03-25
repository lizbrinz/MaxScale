#!/usr/bin/env python

import time
import json
import re
import sys
import socket
import hashlib
import argparse
import subprocess

# Read data as JSON
def read_json():
    nodata = 0
    rbuf = ""
    decoder = json.JSONDecoder()

    while True:
        if nodata > 5:
            exit(1)

        try:
            data = decoder.raw_decode(rbuf)
            rbuf = rbuf[data[1]:]
            print(json.dumps(data[0]))

        except Exception as ex:
            buf = sock.recv(1024)
            if len(buf) > 0:
                rbuf += buf
            else:
                nodata += 1

# Read data as Avro
def read_avro():
    nodata = 0

    while True:
        if nodata > 5:
            exit(1)

        try:
            buf = sock.recv(1024, socket.MSG_DONTWAIT)

            if len(buf) > 0:
                nodata = 0
                sys.stdout.write(buf)

        except:
            nodata += 1
            time.sleep(1)

parser = argparse.ArgumentParser(description = "CDC Binary consumer")
parser.add_argument("--host", dest="host", help="Network address where the connection is made", default="localhost")
parser.add_argument("-P", "--port", dest="port", help="Port where the connection is made", default="4001")
parser.add_argument("-u", "--user", dest="user", help="Username used when connecting", default="")
parser.add_argument("-p", "--password", dest="password", help="Password used when connecting", default="")
parser.add_argument("-f", "--format", dest="format", help="Data transmission format", default="JSON", choices=["AVRO", "JSON"])
parser.add_argument("FILE", help="Requested table name. Must be in the following format: DATABASE.TABLE[.VERSION]")
opts = parser.parse_args(sys.argv[1:])

sock = socket.create_connection([opts.host, opts.port])

# Authentication
auth_string = str(opts.user + ":").encode('hex') + hashlib.sha1(opts.password).hexdigest()
sock.send(auth_string)

# Discard the response
response = sock.recv(1024).encode('utf_8')

# Register as a client as request Avro format data
sock.send("REGISTER UUID=XXX-YYY_YYY, TYPE=" + opts.format)

# Discard the response again
response = sock.recv(1024).encode('utf_8')

# Request a data stream
sock.send("REQUEST-DATA " + opts.FILE)

if opts.format == "JSON":
    read_json()
elif opts.format == "AVRO":
    read_avro()
