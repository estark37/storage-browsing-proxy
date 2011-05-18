# -*- coding: cp1252 -*-
# <PythonProxy.py>
#
#Copyright (c) <2009> <Fábio Domingues - fnds3000 in gmail.com>
#
#Permission is hereby granted, free of charge, to any person
#obtaining a copy of this software and associated documentation
#files (the "Software"), to deal in the Software without
#restriction, including without limitation the rights to use,
#copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the
#Software is furnished to do so, subject to the following
#conditions:
#
#The above copyright notice and this permission notice shall be
#included in all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
#OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
#NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
#WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
#OTHER DEALINGS IN THE SOFTWARE.

"""\
Copyright (c) <2009> <Fábio Domingues - fnds3000 in gmail.com> <MIT Licence>

                  **************************************
                 *** Python Proxy - A Fast HTTP proxy ***
                  **************************************

Neste momento este proxy é um Elie Proxy.

Suporta os métodos HTTP:
 - OPTIONS;
 - GET;
 - HEAD;
 - POST;
 - PUT;
 - DELETE;
 - TRACE;
 - CONENCT.

Suporta:
 - Conexões dos cliente em IPv4 ou IPv6;
 - Conexões ao alvo em IPv4 e IPv6;
 - Conexões todo o tipo de transmissão de dados TCP (CONNECT tunneling),
     p.e. ligações SSL, como é o caso do HTTPS.

A fazer:
 - Verificar se o input vindo do cliente está correcto;
   - Enviar os devidos HTTP erros se não, ou simplesmente quebrar a ligação;
 - Criar um gestor de erros;
 - Criar ficheiro log de erros;
 - Colocar excepções nos sítios onde é previsível a ocorrência de erros,
     p.e.sockets e ficheiros;
 - Rever tudo e melhorar a estrutura do programar e colocar nomes adequados nas
     variáveis e métodos;
 - Comentar o programa decentemente;
 - Doc Strings.

Funcionalidades futuras:
 - Adiconar a funcionalidade de proxy anónimo e transparente;
 - Suportar FTP?.


(!) Atenção o que se segue só tem efeito em conexões não CONNECT, para estas o
 proxy é sempre Elite.

Qual a diferença entre um proxy Elite, Anónimo e Transparente?
 - Um proxy elite é totalmente anónimo, o servidor que o recebe não consegue ter
     conhecimento da existência do proxy e não recebe o endereço IP do cliente;
 - Quando é usado um proxy anónimo o servidor sabe que o cliente está a usar um
     proxy mas não sabe o endereço IP do cliente;
     É enviado o cabeçalho HTTP "Proxy-agent".
 - Um proxy transparente fornece ao servidor o IP do cliente e um informação que
     se está a usar um proxy.
     São enviados os cabeçalhos HTTP "Proxy-agent" e "HTTP_X_FORWARDED_FOR".

"""

import socket, thread, select, random, time
import storage
from storage import AmazonSQS, BoxDotNet

__version__ = '0.1.0 Draft 1'
BUFLEN = 6000
VERSION = 'Python Proxy/'+__version__
HTTPVER = 'HTTP/1.1'
USE_STORAGE = True
STORAGE_METHOD = 'AmazonSQS'

def check_for_requests(storage):
    c = storage.get_connection()
    if (c == False):
        return
    else:
        print "Processing connection %s"%c
        conn, host, port = c.split()
        thread.start_new_thread(ConnectionHandler, (conn,host,port))

class ConnectionHandler:
    def __init__(self, conn_id, host, port, timeout=60):
        if (STORAGE_METHOD == 'AmazonSQS'):
            self.storage = AmazonSQS()
        else:
            self.storage = BoxDotNet()
        self.host = host
        self.port = int(port)
        self.conn_id = conn_id
        self.timeout = timeout
        self.client_buffer = ''

        tries = 0
        while (1):
            self.requests = self.storage.get_requests_loc(self.conn_id)
            self.responses = self.storage.get_responses_loc(self.conn_id)
            print "requests loc: %s, responses loc: %s"%(self.requests, self.responses)
            if (self.requests == False or self.responses == False):
                time.sleep(1)
                tries += 1
                if (tries >= 10):
                    return
            else:
                break

        data = self.get_base_header()
        if (not data):
            return
        self.method, self.path, self.protocol = data
        if (not self.method or not self.path or not self.protocol):
            return

        if self.method=='CONNECT':
            self.method_CONNECT()
        elif self.method in ('OPTIONS', 'GET', 'HEAD', 'POST', 'PUT',
                             'DELETE', 'TRACE'):
            self.method_others()

        self.target.close

    def get_base_header(self):
        count = 0
        while 1:
            data = self.storage.get(self.requests, True)
            if (data):
                self.client_buffer += data
                end = self.client_buffer.find('\n')
                if end!=-1:
                    break
            count += 1
            if (count > self.timeout):
                return False
        print '%s'%self.client_buffer[:end]#debug
        data = (self.client_buffer[:end+1]).split()
        self.client_buffer = self.client_buffer[end+1:]
        return data

    def method_CONNECT(self):
        self._connect_target(self.path)
        self.client_buffer = ''
        self._read_write()        

    def method_others(self):
        print "Path: %s, host: %s"%(self.path, self.host)
        path = self.path
        self._connect_target(self.host)
        data = '%s %s %s\n'%(self.method, path, self.protocol)+ self.client_buffer
        self.target.send(data)

        self.client_buffer = ''
        self._read_write()

    def _connect_target(self, host):
        i = host.find(':')
        if i!=-1:
            port = int(host[i+1:])
            host = host[:i]
        else:
            port = self.port
        print "Host: %s, port: %s"%(host,port)
        (soc_family, _, _, _, address) = socket.getaddrinfo(host, port)[0]
        self.target = socket.socket(soc_family)
        self.target.connect(address)

    def _read_write(self):
        time_out_max = self.timeout/3
        socs = [self.target]
        count = 0
        while 1:
            count += 1
            (recv, _, error) = select.select(socs, [], socs, 3)
            if error:
                break
            out = self.target
            if recv:
                for in_ in recv:
                    data = in_.recv(BUFLEN)
                    if data:
                        if in_ is self.target:
                            self.storage.put(self.responses, data, True)
                        else:    
                            out.send(data)
                        count = 0
            print "Getting messages from requests folder %s"%self.requests
            resp = self.storage.get(self.requests, True)
            if (resp):
                print "Got data: %s"%resp
                out.send(resp)
            if count == time_out_max:
                break

def start_server(host='localhost', port=8081, IPv6=False, timeout=60):
    if IPv6==True:
        soc_type=socket.AF_INET6
    else:
        soc_type=socket.AF_INET
    soc = socket.socket(soc_type)
    soc.bind((host, port))
    print "Serving on %s:%d."%(host, port)#debug
    soc.listen(0)
    
    if (STORAGE_METHOD == 'AmazonSQS'):
        storage = AmazonSQS()
    else:
        storage = BoxDotNet()

    while 1:
        check_for_requests(storage)

if __name__ == '__main__':
    start_server()
