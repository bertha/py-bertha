import collections
import struct
import socket
import os

BERTHA_LIST = 0
BERTHA_PUT = 1
BERTHA_GET = 2
BERTHA_QUIT = 3
BERTHA_SPUT = 4
BERTHA_SGET = 5
BERTHA_SIZE = 6

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

        def quit(self):
                """ QUITs the server """
                sock = self._connect()
                f = sock.makefile()
                f.write(struct.pack("B", BERTHA_QUIT))
                f.flush()
                f.close()
                sock.close()

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

        def put(self, size=None):
                """ Returns an object with attributes f and finish.
                    f is a file object to which you write the data to PUT
                    finish is a method, which when called finalizes the PUT
                    and returns the key of the blob.
                    With size you can optionally specify the size of the blob
                    you will send.  This allows the server to preallocate. """
                sock = self._connect()
                f = sock.makefile()
                if size is None:
                        f.write(struct.pack("B", BERTHA_PUT))
                else:
                        f.write(struct.pack("<BQ", BERTHA_SPUT, size))
                return BerthaPutContext(sock, f)

        def put_file(self, file_obj, size=None):
                """ PUTs the file() object <file_obj> on the server.
                    If the object has a fileno(), we will fstat it to determine
                    its size and send it along the request such that the server
                    can preallocate the space.
                    If a size is specified via <size>, we will send that
                    size instead.  However, we will still send as much as
                    we can read, whether that is less or more than size. """
                sock = self._connect()
                f = sock.makefile()
                if size is None and hasattr(file_obj, 'fileno'):
                        stat_size = os.fstat(file_obj.fileno()).st_size
                        if stat_size != 0:
                                size = stat_size
                if size is None:
                        f.write(struct.pack("B", BERTHA_PUT))
                else:
                        f.write(struct.pack("<BQ", BERTHA_SPUT, size))
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
                f.write(struct.pack("<BQ", BERTHA_SPUT, len(s)))
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

        def sget(self, key):
                """ SGETs a file from the server.  That is: a pair (fd, s)
                    is returned, where fd is a file object to read from
                    and s is the length of fd """
                sock = self._connect()
                f = sock.makefile()
                f.write(struct.pack("B", BERTHA_SGET))
                f.write(hex_to_str(key))
                f.flush()
                sock.shutdown(socket.SHUT_WR)
                raw_size = f.read(8)
                if len(raw_size) == 0:
                        raise KeyError
                size = struct.unpack("<Q", raw_size)[0]
                return (f, size)

        def size(self, key):
                """ Returns the size of the blob on the server.
                    When the file does not exist, a KeyError is raised. """
                sock = self._connect()
                f = sock.makefile()
                f.write(struct.pack("B", BERTHA_SIZE))
                f.write(hex_to_str(key))
                f.flush()
                sock.shutdown(socket.SHUT_WR)
                raw_size = f.read(8)
                if len(raw_size) == 0:
                        raise KeyError
                size = struct.unpack("<Q", raw_size)[0]
                return size

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
