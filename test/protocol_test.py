#!/usr/local/bin/python
"""
Test all the protocol commands and variations on them to establish the behavior of a
memcached server.  Also a good test to see if my server matches that.

==========================================================================================

Copyright 2011 James Yates Farrimond. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are
permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of
      conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list
      of conditions and the following disclaimer in the documentation and/or other materials
      provided with the distribution.

THIS SOFTWARE IS PROVIDED BY JAMES YATES FARRIMOND ''AS IS'' AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL JAMES YATES FARRIMOND OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are those of the
authors and should not be interpreted as representing official policies, either expressed
or implied, of James Yates Farrimond.
"""
import contextlib
import socket
import unittest

HOST = '127.0.0.1'        # The remote host
PORT = 11211              # The same port as used by the server

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
