#!/usr/bin/env python

import boto, uuid
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message

class StorageService:
    def new_connection(self, conn_id):
        raise NotImplementedError("Abstract class")
    def get_connection(self):
        raise NotImplementedError("Abstract class")
    def get_connections_loc(self):
        raise NotImplementedError("Abstract class")
    def get_requests_loc(self):
        raise NotImplementedError("Abstract class")
    def get_responses_loc(self):
        raise NotImplementedError("Abstract class")

    """ use_seq_num forces FIFO ordering of messages.
    In order for one party to receive messages in a FIFO order,
    both parties must call both get() and put() with use_seq_num = True.
    For HTTP requests/responses, FIFO ordering is important; for retrieving
    connection IDs or IPs of clients wanting to use a Flash proxy,
    FIFO isn't important."""
    def put(self, data, use_seq_num = False):
        raise NotImplementedError("Abstract class")
    def get(self, use_seq_num = False):
        raise NotImplementedError("Abstract class")

class AmazonSQS(StorageService):
    def __init__(self):
        self.last_msg = -1
        self.buffered = []
        self.msg_num = 0
        self.access_key_id = ""
        self.secret_access_key = ""
        self.conn = SQSConnection(self.access_key_id, self.secret_access_key, True, None, None, None, None, None)

    def new_connection(self, conn_id):
        conns = self.conn.create_queue("connections")
        self.put(conns, conn_id)

    def get_connection(self):
        conns = self.conn.create_queue("connections")
        return self.get(conns)

    def get_connections_loc(self):
        return self.conn.create_queue("connections")

    def get_responses_loc(self, conn_id):
        return self.conn.create_queue("%s_response"%conn_id)

    def get_requests_loc(self, conn_id):
        return self.conn.create_queue("%s_request"%conn_id)

    def put(self, q, data, use_seq_num = False):
        m = Message()
        if (use_seq_num):
            data = str(self.msg_num) + " " + data
            self.msg_num += 1
        m.set_body(data)
#        print "Putting data: %s"%data
        status = q.write(m)
        if (status == False):
            print "Put failed"
            return False
        return True

    def get(self, q, use_seq_num = False):
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
            if (not use_seq_num):
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
