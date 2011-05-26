#!/usr/bin/env python

import boto, uuid
import urllib
import urllib2
import mimetools
import mimetypes
import os

from xml.dom import minidom
import xml.dom

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
    def get_place(self, place_name):
        raise NotImplementedError("Abstract class")

    """ use_seq_num forces FIFO ordering of messages.
    In order for one party to receive messages in a FIFO order,
    both parties must call both get() and put() with use_seq_num = True.
    For HTTP requests/responses, FIFO ordering is important; for retrieving
    connection IDs or IPs of clients wanting to use a Flash proxy,
    FIFO isn't important."""
    def put(self, place, data, use_seq_num = False):
        raise NotImplementedError("Abstract class")
    def get(self, place, use_seq_num = False):
        raise NotImplementedError("Abstract class")

    def delete(self, place, data):
        raise NotImplementedError("Abstract class")

    def create_local_file(self, name, contents):
        f = open("%s/%s"%(os.getcwd(),name), 'w')
        f.write(contents)
        f.close()

    def delete_local_file(self, name):
        os.remove("%s/%s"%(os.getcwd(),name))

""" Some parts cribbed from boxdotnet.py from http://developers.box.net/w/page/12923917/ApiExamples"""
class BoxDotNet(StorageService):
    def __init__(self):
        self.api_key = "bo8ktxdnlkco5dc55xf6plpggs8flece"
        self.token = "zem048m381vspgafljvbipk7s00iebmn"
        self.download_url = "https://www.box.net/api/1.0/download/%s/"%self.token
        self.upload_url = "https://upload.box.net/api/1.0/upload/%s/"%self.token
        self.url = "https://www.box.net/api/1.0/rest?action="
        self.requests_folder = "86034303"
        self.responses_folder = "86034411"
        self.connections_folder = "86041821"
        # Map folder IDs to the last sequence number that we saw in that folder
        self.last_get_seq = {}
        self.last_put_seq = {}
    #The box.net return status codes are all over the show                                                                   
    # method_name : return_value_that_is_ok                                                                                  
        self.RETURN_CODES = {
            'get_ticket'        :   'get_ticket_ok',
            'get_auth_token'    :   'get_auth_token_ok',
            'get_account_tree'  :   'listing_ok',
            'logout'            :   'logout_ok',
            'create_folder'     :   'create_ok',
            'upload'            :   'upload_ok',
            'delete'            :   's_delete_node',
            'get_versions'      :   's_get_versions'
            }
    
    def check_for_error(self, xmldoc, action):
        status = xmldoc.childNodes[0].childNodes[0].childNodes[0].nodeValue
        if (status == self.RETURN_CODES[action]):
            return False
        return True

    def delete(self, place, data):
        raise NotImplementedError("Use BoxDotNetSmallData if you need this functionality.")

    def make_url(self, action, extra_args = {}):
        url = "%s%s&api_key=%s&auth_token=%s"%(self.url, action, self.api_key, self.token)
        url = "%s&%s"%(url, urllib.urlencode(extra_args))
#        for arg, val in extra_args.iteritems():
#url = url + "&%s=%s"%(arg, urllib.urlencode(val))

        return url

    def make_request(self, url):
        f = urllib.urlopen(url, proxies = {})
        data = f.read()
        if (data):
            xmldoc = minidom.parseString(data)
            return xmldoc
        return False

    def get_content_type(self, filename):
        return mimetypes.guess_type(filename)[0] or 'application/octet-stream'      
        
    def upload_file(self, name, folder_id):
        url = "%s%s"%(self.upload_url, folder_id)
        filename = "%s/%s"%(os.getcwd(), name)

        # construct POST data                                                                                                
        boundary = mimetools.choose_boundary()
        body = ""

        # filename                                                                                                           
        body += "--%s\r\n" % (boundary)
        body += 'Content-Disposition: form-data; name="share"\r\n\r\n'
        body += "1\r\n" # share = 1

        body += "--%s\r\n" % (boundary)
        body += "Content-Disposition: form-data; name=\"file\";"
        body += " filename=\"%s\"\r\n" % filename
        body += "Content-Type: %s\r\n\r\n" % self.get_content_type(filename)

        fp = file(filename, "rb")
        data = fp.read()
        fp.close()

        postData = body.encode("utf_8") + data + \
            ("\r\n--%s--" % (boundary)).encode("utf_8")

        ph = urllib2.ProxyHandler({})
        opener = urllib2.build_opener(ph)
        urllib2.install_opener(opener)
        request = urllib2.Request(url)
        request.add_data(postData)
        request.add_header("Content-Type", \
            "multipart/form-data; boundary=%s" % boundary)
        response = urllib2.urlopen(request)
        rspXML = response.read()

        if (rspXML):
            xmldoc = minidom.parseString(rspXML)
            if (not self.check_for_error(xmldoc, "upload")):
                return True
        return False

    def download_file(self, file_id, version_id = False):
        url = "%s%s"%(self.download_url, file_id)
        if (version_id):
            url = "%s/%s"%(url, version_id)
        print "Downloading file from %s"%url
        request = urllib2.Request(url)
        ph = urllib2.ProxyHandler({})
        opener = urllib2.build_opener(ph)
        urllib2.install_opener(opener)
        response = urllib2.urlopen(request)
        return response.read()

    # ID is typically just a random number, name includes the host and port
    def new_connection(self, conn_id, conn_name):
        self.create_local_file(conn_name, "")
        self.upload_file(conn_name, self.connections_folder)
        ######
        # TODO: check errors for the following requests
        ######
        url = self.make_url("create_folder", {"parent_id": self.requests_folder, "name": conn_id, "share": "1"})
        self.make_request(url)
        url = self.make_url("create_folder", {"parent_id": self.responses_folder, "name": conn_id, "share": "1"})
        self.make_request(url)
        self.delete_local_file(conn_name)
        return (self.get_requests_loc(conn_id), self.get_responses_loc(conn_id))

    def delete_file(self, file_id):
        url = self.make_url("delete", {"target": "file", "target_id": file_id})
        self.make_request(url)

    def get_connection(self):
        url = self.make_url("get_account_tree", {"folder_id": self.connections_folder, "params[]": "nozip"})
        xmldoc = self.make_request(url)
        if (xmldoc):
            if (not self.check_for_error(xmldoc, "get_account_tree")):
                print "Getting connection"
                folder_children = xmldoc.childNodes[0].childNodes[1].childNodes[0].childNodes
                if (len(folder_children) <= 1):
                    return False
                files = folder_children[1]
                if (len(files.childNodes) > 0):
                    file_node = files.childNodes[0]
                    conn_id = file_node.attributes["file_name"].nodeValue
                    file_id = file_node.attributes["id"].nodeValue
                    self.delete_file(file_id)
                    return conn_id

    def get_connections_loc(self):
        return self.connections_folder

    def get_loc(self, conn_id, root):
        url = self.make_url("get_account_tree", {"folder_id": root, "params[]": "nozip"})
        xmldoc = self.make_request(url)
        if (xmldoc):
            if (not self.check_for_error(xmldoc, "get_account_tree")):
                folder_children = xmldoc.childNodes[0].childNodes[1].childNodes[0].childNodes[1].childNodes
                folder_children = filter(lambda f: f.attributes["name"].nodeValue == conn_id, folder_children)
                if (len(folder_children) > 0):
                    return folder_children[0].attributes["id"].nodeValue
        return False

    def get_place(self, place_name):
        loc = self.get_loc(place_name, 0)
        if (loc == False):
            url = self.make_url("create_folder", {"parent_id": "0", "name": place_name, "share": "1"})
            xmldoc = self.make_request(url)
            if (xmldoc):
                if (not self.check_for_error(xmldoc, "create_folder")):
                    return xmldoc.childNodes[0].childNodes[1].childNodes[0].childNodes[0].nodeValue
        else:
            return loc


    def get_requests_loc(self, conn_id):
        return self.get_loc(conn_id, self.requests_folder)
    def get_responses_loc(self, conn_id):
        return self.get_loc(conn_id, self.responses_folder)

    """ For Box.net API, we always use sequence numbers and enforce FIFO. """
    def put(self, place, data, use_seq_num = True):        
        if (self.last_put_seq.has_key(place)):
            next_seq_num = self.last_put_seq[place] + 1
        else:
            self.last_put_seq[place] = -1
            next_seq_num = 0
        filename = "%d"%next_seq_num
        self.create_local_file(filename, data)
        self.upload_file(filename, place)
        self.last_put_seq[place] = next_seq_num

    def get(self, place, use_seq_num = True):
        if (self.last_get_seq.has_key(place)):
            next_seq_num = self.last_get_seq[place] + 1
        else:
            self.last_get_seq[place] = -1
            next_seq_num = 0
        url = self.make_url("get_account_tree", {"folder_id": place, "params[]": "nozip"})
        xmldoc = self.make_request(url)
        files = []
        if (xmldoc):
            if (not self.check_for_error(xmldoc, "get_account_tree")):
                folder_children = xmldoc.childNodes[0].childNodes[1].childNodes[0].childNodes
                for f in folder_children:
                    if (f.nodeName == "files"):
                        files = f.childNodes
                if (not files or len(files) == 0):
                    return False
                # Find the file whose name is the next sequence number we're expecting in this place
                files = map(lambda f: (f.attributes["file_name"].nodeValue, f.attributes["id"].nodeValue), files)
                files = filter(lambda f: f[0] == "%d"%next_seq_num, files)
                if (len(files) == 0):
                    return False

                file = files[0][1]
                # download the earliest version of a file and delete it
                url = self.make_url("get_versions", {"target": "file", "target_id": file})
                xmldoc = self.make_request(url)
                if (not self.check_for_error(xmldoc, "get_versions")):
                    versions = xmldoc.childNodes[0].childNodes[1].childNodes
                    versions = map(lambda v: (v.attributes["updated"].nodeValue, v.attributes["version_id"].nodeValue), versions)
                    if (len(versions) == 0):
                        use_version = False
                    else:
                        versions.sort()
                        use_version = versions[len(versions)-1][1]
                    data = self.download_file(file, use_version)
                    self.last_get_seq[place] = next_seq_num
                    url = self.make_url("delete", {"target":"file", "target_id":file})
                    self.make_request(url)
                    return data

""" A subclass of BoxDotNet for 'small' data. When the data stored is short enough to be contained in the file name and sequence numbers (FIFO) aren't necessary, use this class. """
class BoxDotNetSmallData(BoxDotNet):
    def put(self, place, data):
        filename = data
        self.create_local_file(filename, "")
        self.upload_file(filename, place)

    def get(self, place, use_seq_num = False):
        url = self.make_url("get_account_tree", {"folder_id": place, "params[]": "nozip"})
        xmldoc = self.make_request(url)
        files = []
        if (xmldoc):
            if (not self.check_for_error(xmldoc, "get_account_tree")):
                folder_children = xmldoc.childNodes[0].childNodes[1].childNodes[0].childNodes
                for f in folder_children:
                    if (f.nodeName == "files"):
                        files = f.childNodes
                if (not files or len(files) == 0):
                    return False
                file = (files[0].attributes["file_name"].nodeValue, files[0].attributes["id"].nodeValue)

                url = self.make_url("delete", {"target":"file", "target_id":file[1]})
                self.make_request(url)
                return file[0]

    def delete(self, place, data):
        url = self.make_url("get_account_tree", {"folder_id": place, "params[]": "nozip"})
        xmldoc = self.make_request(url)
        files = []
        if (xmldoc):
            if (not self.check_for_error(xmldoc, "get_account_tree")):
                folder_children = xmldoc.childNodes[0].childNodes[1].childNodes[0].childNodes
                for f in folder_children:
                    if (f.nodeName == "files"):
                        files = f.childNodes
                if (not files or len(files) == 0):
                    return
                files = filter(lambda f: f.attributes["file_name"].nodeValue == data, files)            
                file = files[0].attributes["id"].nodeValue
                url = self.make_url("delete", {"target":"file", "target_id":file})
                self.make_request(url)


class AmazonSQS(StorageService):
    def __init__(self):
        self.last_msg = -1
        self.buffered = []
        self.msg_num = 0
        self.access_key_id = ""
        self.secret_access_key = ""
        self.conn = SQSConnection(self.access_key_id, self.secret_access_key, True, None, None, None, None, None)

    def new_connection(self, conn_id, conn_name):
        conns = self.conn.create_queue("connections")
        self.put(conns, conn_name)
        return (self.get_requests_loc(conn_id), self.get_responses_loc(conn_id))

    def get_place(self, place_name):
        return self.create_queue(place_name)

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

    def delete(self, place, data):
        msgs = place.get_messages()
        for m in msgs:
            if (m.get_body() == data):
                place.delete_message(m)


class AmazonS3(StorageService):
    def __init__(self):
        self.last_msg = -1
        self.buffered = []
        self.msg_num = 0
        self.access_key_id = ""
        self.secret_access_key = ""
        self.conn =  boto.connect_s3(self.access_key_id, self.secret_access_key)

    def new_connection(self, conn_id, conn_name):
        conns = self.conn.create_bucket("connections294s")
        self.put(conns, conn_name)
        return (self.get_requests_loc(conn_id), self.get_responses_loc(conn_id))

    def get_place(self, place_name):
        return self.create_bucket(place_name)

    def get_connection(self):
        conns = self.conn.create_bucket("connections294s")
        return self.get(conns)

    def get_connections_loc(self):
        return self.conn.create_bucket("connections294s")

    def get_responses_loc(self, conn_id):
        return self.conn.create_bucket("%s_response"%conn_id)

    def get_requests_loc(self, conn_id):
        return self.conn.create_bucket("%s_request"%conn_id)

    def put(self, q, data, use_seq_num = False):
       # print "Putting data:"
	#filename = "testcs294file"
	#FILE = open(filename,"w")
	#status = FILE.writelines(m)
    	#FILE.close()
	if (use_seq_num):
            data = str(self.msg_num) + " " + data
            self.msg_num += 1
	    print "data"
	    print data
        import sys
	def percent_cb(complete, total):
    		sys.stdout.write('.')
    		sys.stdout.flush()

	from boto.s3.key import Key
	k = Key(q)
	k.key = 'testcs294file'
	status = k.set_contents_from_string(data)
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
	import sys
	def percent_cb(complete, total):
    		sys.stdout.write('.')
    		sys.stdout.flush()

	import time
	
	from boto.s3.key import Key
	k = Key(q)
	k.key = 'testcs294file'
        if (not k.exists()):
            return False
	m = k.get_contents_as_string()
        if (m == None):
            return False
        else:
	   q.delete_key(k)
	
	   if (not use_seq_num):
		return m
           else:
		msg_num, sep, msg = m.partition(" ")
		msg_num = int(msg_num)
		#print msg_num
		#print self.last_msg
                if (msg_num == self.last_msg + 1):
                   # print "Using msg %d"%msg_num
                    self.last_msg = msg_num
               	    return msg
                else:
                   # print "Buffering msg %d, last_msg: %d"%(msg_num, self.last_msg)
                    self.buffered.append((msg_num, msg))
                    self.buffered.sort()
		   # print self.buffered
                
       
    def create_bucket(self, name):
        return self.conn.create_bucket(name)



    def delete(self, place, data):
        bucket_list = place.list()
        for m in bucket_list:
	    k = m.key
	    str = k.get_contents_as_string()
            if (str == data):
                place.delete_key(k)


"""
StorageQueue is a redundant queue distributed over multiple storage services.
enqueue() tries to insert a value into all available storage services.
dequeue() tries to get a value from any available storage service, and once it gets a value,
it deletes this value from all other available services.
"""
class StorageQueue:
    # storages is a list of names of StorageServices (i.e. storages = ['AmazonSQS', 'BoxDotNetSmallData'])
    def __init__(self, storages, place_name):
        storages = map(lambda s: eval(s + "()"), storages)
        places = map(lambda storage: storage.get_place(place_name), storages)
        self.storages = zip(storages, places)
    
    def enqueue(self, data):
        for (storage, place) in self.storages:
            storage.put(place, data)

    def dequeue(self):
        for (storage, place) in self.storages:
            data = storage.get(place)
            if (data):
                self.delete_from_all(data)
                break
        return data

    def delete_from_all(self, data):
        for (storage, place) in self.storages:
            storage.delete(place, data)
            
