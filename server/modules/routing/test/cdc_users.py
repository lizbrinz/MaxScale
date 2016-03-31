#!/usr/bin/env python3

import sys, binascii, hashlib, argparse

parser = argparse.ArgumentParser(description = "CDC User manager", epilog = "Append the output of this program to /var/cache/maxscale/<service name>/cdcusers")
parser.add_argument("USER", help="Username")
parser.add_argument("PASSWORD", help="Password")
opts = parser.parse_args(sys.argv[1:])

print((binascii.b2a_hex((opts.USER + ":").encode()) + hashlib.sha1(opts.PASSWORD.encode()).hexdigest().encode()).decode('ascii'))
