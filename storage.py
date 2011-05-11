#!/usr/bin/env python

import boto, uuid
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message

class StorageService:
    def put(self, data):
        raise NotImplementedError("Abstract class")
    def get(self):
        raise NotImplementedError("Abstract class")

class AmazonSQS(StorageService):
    def __init__(self):
        self.access_key_id = ""
        self.secret_access_key = ""
        self.conn = SQSConnection(self.access_key_id, self.secret_access_key, True, None, None, None, None, None)
    def put(self, q, data):
        m = Message()
        m.set_body(data)
        print "Putting data: %s"%data
        status = q.write(m)
        print "Status:"
        print status
        if (status == False):
            print "Put failed"
            return False
        return True
    def get(self, q):
        m = q.read()
        if (m == None):
            return False
        else:
            q.delete_message(m)
            return m.get_body()
    def create_queue(self, name):
        return self.conn.create_queue(name)
