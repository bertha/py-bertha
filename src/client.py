import collections
import struct
import socket

BERTHA_LIST = 0
BERTHA_PUT = 1
BERTHA_GET = 2

def str_to_hex(s):
        return ''.join((hex(ord(c))[2:].zfill(2) for c in s))

def hex_to_str(s):
        return ''.join((chr(int(s[2*i:2*(i+1)], 16))
                        for i in xrange(len(s) / 2)))

class BerthaPutContext(object):
        """ Context object returned by BerthaClient.put """
        def __init__(self, sock, f):
                self.__sock = sock
                self.f = f
        def finish(self):
                """ Finish the PUT.  Returns the key of the blob """
                self.f.flush()
                self.__sock.shutdown(socket.SHUT_WR)
                key = self.f.read(32)
                self.f.close()
                self.__sock.close()
                self.f = None
                return str_to_hex(key)

class BerthaClient(object):
        def __init__(self, host='localhost', port=819):
                """ Creates a new BerthaClient """
                self.host = host
                self.port = port
                self._addr = None

        def list(self):
                """ LISTs all keys on the server """
                sock = self._connect()
                f = sock.makefile()
                f.write(struct.pack("B", BERTHA_LIST))
                f.flush()
                while True:
                        s = f.read(32)
                        if len(s) != 32:
                                break
                        yield str_to_hex(s)
                f.close()
                sock.close()

        def put(self):
                """ Returns an object with attributes f and finish.
                    f is a file object to which you write the data to PUT
                    finish is a method, which when called finalizes the PUT
                    and returns the key of the blob. """
                sock = self._connect()
                f = sock.makefile()
                f.write(struct.pack("B", BERTHA_PUT))
                return BerthaPutContext(sock, f)

        def put_file(self, file_obj):
                """ PUTs the file() object <file_obj> on the server """
                sock = self._connect()
                f = sock.makefile()
                f.write(struct.pack("B", BERTHA_PUT))
                while True:
                        buf = file_obj.read(4096)
                        if not buf:
                                break
                        f.write(buf)
                f.flush()
                sock.shutdown(socket.SHUT_WR)
                key = f.read(32)
                f.close()
                sock.close()
                return str_to_hex(key)
        
        def put_str(self, s):
                """ PUTs a string on the server """
                sock = self._connect()
                f = sock.makefile()
                f.write(struct.pack("B", BERTHA_PUT))
                f.write(s)
                f.flush()
                sock.shutdown(socket.SHUT_WR)
                key = f.read(32)
                f.close()
                sock.close()
                return str_to_hex(key)

        def get(self, key):
                """ GETs a file from the server """
                sock = self._connect()
                f = sock.makefile()
                f.write(struct.pack("B", BERTHA_GET))
                f.write(hex_to_str(key))
                f.flush()
                sock.shutdown(socket.SHUT_WR)
                return f

        def _connect(self):
                s = None
                for res in socket.getaddrinfo(self.host,
                                              self.port,
                                              socket.AF_UNSPEC,
                                              socket.SOCK_STREAM):
                        af, socktype, proto, canonname, sa = res
                        try:
                                s = socket.socket(af, socktype, proto)
                        except socket.error, msg:
                                s = None
                                continue
                        try:
                                s.connect(sa)
                        except socket.error, msg:
                                s.close()
                                s = None
                                continue
                        return s
                if s is None:
                        raise ValueError, "Couldn't connect to %s:%s" %(
                                        self.host, self.port)
