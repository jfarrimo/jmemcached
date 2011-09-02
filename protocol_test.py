#!/usr/local/bin/python
import contextlib
import socket
import unittest

HOST = '127.0.0.1'        # The remote host
PORT = 11212              # The same port as used by the server

class SimpleSocket(object):
    def __init__(self, host, port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((HOST, PORT))

    def close(self):
        self.s.close()

    def send_receive(self, msg):
        self.s.send(msg)
        reply = self.s.recv(4096)
        return reply

@contextlib.contextmanager
def get_socket(host, port):
    s = SimpleSocket(host, port)
    try:
        yield s
    finally:
        s.close()

class TestProtocol(unittest.TestCase):

    def setUp(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("delete test_key\r\n")

    def test_set(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            self.assertTrue(ret == "STORED\r\n")

    def test_cas(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            self.assertTrue(ret == "STORED\r\n")

            ret = s.send_receive("gets test_key\r\n")
            casunique = ret.split()[4].split('\r')[0]

            ret = s.send_receive("cas test_key 0 0 5 %s\r\n23456\r\n" % casunique)
            self.assertTrue(ret == "STORED\r\n")

            ret = s.send_receive("get test_key\r\n")
            self.assertTrue(ret == "VALUE test_key 0 5\r\n23456\r\nEND\r\n")

    def test_cas_exists(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")

            ret = s.send_receive("gets test_key\r\n")
            casunique = ret.split()[4].split('\r')[0]

            ret = s.send_receive("set test_key 0 0 5\r\n67890\r\n")

            ret = s.send_receive("cas test_key 0 0 5 %s\r\n23456\r\n" % casunique)
            self.assertTrue(ret == "EXISTS\r\n")

            ret = s.send_receive("get test_key\r\n")
            self.assertTrue(ret == "VALUE test_key 0 5\r\n67890\r\nEND\r\n")

    def test_cas_not_exists(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("cas test_key 0 0 5 121212\r\n23456\r\n")
            self.assertTrue(ret == "NOT_FOUND\r\n")

    def test_add(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("add test_key 0 0 5\r\n12345\r\n")
            self.assertTrue(ret == "STORED\r\n")

    def test_add_exists(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            self.assertTrue(ret == "STORED\r\n")

            ret = s.send_receive("add test_key 0 0 5\r\n67890\r\n")
            self.assertTrue(ret == "NOT_STORED\r\n")

    def test_replace(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            ret = s.send_receive("replace test_key 0 0 5\r\n34567\r\n")
            self.assertTrue(ret == "STORED\r\n")

    def test_replace_not_exists(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("replace test_key 0 0 5\r\n34567\r\n")
            self.assertTrue(ret == "NOT_STORED\r\n")

    def test_prepend(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            ret = s.send_receive("prepend test_key 0 0 1\r\n0\r\n")
            self.assertTrue(ret == "STORED\r\n")

    def test_prepend_not_exist(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("prepend test_key 0 0 1\r\n0\r\n")
            self.assertTrue(ret == "NOT_STORED\r\n")

    def test_append(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            ret = s.send_receive("append test_key 0 0 1\r\n6\r\n")
            self.assertTrue(ret == "STORED\r\n")

    def test_append_not_exist(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("append test_key 0 0 1\r\n0\r\n")
            self.assertTrue(ret == "NOT_STORED\r\n")

    def test_increment(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            ret = s.send_receive("incr test_key 1\r\n")
            self.assertTrue(ret == "12346\r\n")

    def test_increment_not_exist(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("incr test_key 1\r\n")
            self.assertTrue(ret == "NOT_FOUND\r\n")

    def test_increment_not_number(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\naaaaa\r\n")
            ret = s.send_receive("incr test_key 1\r\n")
            self.assertTrue(ret == "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")

    def test_increment_partly_number(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 8\r\n123aaaaa\r\n")
            ret = s.send_receive("incr test_key 1\r\n")
            self.assertTrue(ret == "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")

    def test_decrement(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            ret = s.send_receive("decr test_key 1\r\n")
            self.assertTrue(ret == "12344\r\n")

    def test_decrement_not_exist(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("decr test_key 1\r\n")
            self.assertTrue(ret == "NOT_FOUND\r\n")

    def test_decrement_not_number(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\naaaaa\r\n")
            ret = s.send_receive("decr test_key 1\r\n")
            self.assertTrue(ret == "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")

    def test_decrement_partly_number(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 8\r\n123aaaaa\r\n")
            ret = s.send_receive("decr test_key 1\r\n")
            self.assertTrue(ret == "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")

    def test_get(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            ret = s.send_receive("get test_key\r\n")
            self.assertTrue(ret == "VALUE test_key 0 5\r\n12345\r\nEND\r\n")

    def test_get_not_exist(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("get test_key\r\n")
            self.assertTrue(ret == "END\r\n")

    def test_gets(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            ret = s.send_receive("gets test_key\r\n")
            self.assertTrue(ret[:19] == "VALUE test_key 0 5 ")
            self.assertTrue(ret[-14:] == "\r\n12345\r\nEND\r\n")

    def test_gets_not_exist(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("gets test_key\r\n")
            self.assertTrue(ret == "END\r\n")

    def test_delete(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("set test_key 0 0 5\r\n12345\r\n")
            ret = s.send_receive("delete test_key 0\r\n")
            self.assertTrue(ret == "DELETED\r\n")

    def test_delete_not_exists(self):
        with get_socket(HOST, PORT) as s:
            ret = s.send_receive("delete test_key 0\r\n")
            self.assertTrue(ret == "NOT_FOUND\r\n")

    def test_stats(self):
        with get_socket(HOST, PORT) as s:
            s.send_receive("stats\r\n")

    def test_quit(self):
        with get_socket(HOST, PORT) as s:
            s.send_receive("quit\r\n")

    def test_bad_command(self):
        with get_socket(HOST, PORT) as s:
            s.send_receive("flub\r\n")

if __name__ == "__main__":
    unittest.main()
