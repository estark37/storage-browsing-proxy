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
        self.last_msg = -1
        self.buffered = []
        self.msg_num = 0
        self.access_key_id = ""
        self.secret_access_key = ""
        self.conn = SQSConnection(self.access_key_id, self.secret_access_key, True, None, None, None, None, None)
    def put(self, q, data, use_msg_num = False):
        m = Message()
        if (use_msg_num):
            data = str(self.msg_num) + " " + data
            self.msg_num += 1
        m.set_body(data)
#        print "Putting data: %s"%data
        status = q.write(m)
        if (status == False):
            print "Put failed"
            return False
        return True
    def get(self, q, use_msg_num = False):
        if (len(self.buffered) > 0 and self.buffered[0][0] == self.last_msg + 1):
            data = self.buffered[0][1]
            self.buffered = self.buffered[1:]
            self.last_msg += 1
            return data
        m = q.read()
        if (m == None):
            return False
        else:
            q.delete_message(m)
            if (not use_msg_num):
                return m.get_body()
            else:
                msg_num, sep, msg = m.get_body().partition(" ")
                msg_num = int(msg_num)
                if (msg_num == self.last_msg + 1):
#                    print "Using msg %d"%msg_num
                    self.last_msg = msg_num
                    return msg
                else:
#                    print "Buffering msg %d, last_msg: %d"%(msg_num, self.last_msg)
                    self.buffered.append((msg_num, msg))
                    self.buffered.sort()
    def create_queue(self, name):
        return self.conn.create_queue(name)
