#!/usr/bin/env python

import storage
from storage import AmazonSQS
import time
import httplib
import urllib

DEFAULT_FACILITATOR = "173.255.221.44"
DEFAULT_PORT = 9002

def post_client(client):
    http = httplib.HTTPConnection(DEFAULT_FACILITATOR)
    print "Posting client: %s"%client
    http.request("POST", "/", urllib.urlencode({"client": client}))
    http.close()

def poll():
    storage = AmazonSQS()
    clients = storage.create_queue("flash_ad_clients")
    while 1:
        print "Looking for clients..."
        client = storage.get(clients)
        if (client):
            print "Found a client: %s"%client
            post_client(client)
        time.sleep(1)

if __name__ == '__main__':
    poll()
