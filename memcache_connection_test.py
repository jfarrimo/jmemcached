#!/usr/local/bin/python
import memcache_connection
import memcache_protocol
import memory_cache
import socket
import unittest

class MockSock(object):
    def __init__(self, buf='', raise_on_access=False, write_partial=False):
        self.raise_on_access = raise_on_access
        self.buf = buf
        self.write_partial = write_partial

    def setblocking(self, blocking):
        pass

    def close(self):
        pass

    def recv(self, bytes):
        if self.raise_on_access:
            raise socket.error( ('arg0', 'arg1') )
        else:
            return self.buf

    def send(self, buf):
        if self.raise_on_access:
            raise socket.error( ('arg0', 'arg1') )
        elif self.write_partial:
            return int(len(buf)/2)
        else:
            return len(buf)

class TestProtocolBase(unittest.TestCase):

    def setUp(self):
        self.sock = MockSock()
        self.stats = memcache_connection.ConnectionStats()
        self.mc = memory_cache.Memcached(self.stats, max_bytes=1024*1024*1024)
        self.mcsock = memcache_connection.MemcachedSocket(self.sock, 'address', self.stats, self.mc)

    def test_read(self):
        self.sock.buf = "stats\r\n"
        self.assertTrue(self.mcsock.handle_read() == self.mcsock.FINISHED)

    def test_read_partial(self):
        self.sock.buf = "stat"
        self.assertTrue(self.mcsock.handle_read() == self.mcsock.CONTINUE)

        self.sock.buf = "s\r\n"
        self.assertTrue(self.mcsock.handle_read() == self.mcsock.FINISHED)

    def test_read_socket_error(self):
        self.sock.raise_on_access = True
        self.assertTrue(self.mcsock.handle_read() == self.mcsock.ERROR)

    def test_read_bad_request(self):
        self.sock.buf = "flub\r\n"
        self.assertTrue(self.mcsock.handle_read() == self.mcsock.FINISHED)

    def test_read_quit(self):
        self.sock.buf = "quit\r\n"
        self.assertTrue(self.mcsock.handle_read() == self.mcsock.QUIT)

    def test_read_connection_closed(self):
        self.assertTrue(self.mcsock.handle_read() == self.mcsock.ERROR)

    def test_write(self):
        self.mcsock.reply = "STORED\r\n"
        self.assertTrue(self.mcsock.handle_write() == self.mcsock.FINISHED)

    def test_write_socket_error(self):
        self.sock.raise_on_access = True
        self.assertTrue(self.mcsock.handle_write() == self.mcsock.ERROR)

    def test_write_partial(self):
        self.sock.write_partial = True
        self.mcsock.reply = "STORED\r\n"
        self.assertTrue(self.mcsock.handle_write() == self.mcsock.OK)

if __name__ == "__main__":
    unittest.main()
