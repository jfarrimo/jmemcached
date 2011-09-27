#!/usr/local/bin/python
import memcache_logging as mcl
import unittest

class TestLogging(unittest.TestCase):

    def setUp(self):
        self.logger = mcl.MemcachedLogger( ('127.0.0.1', 11211) )

    def test_initialize(self):
        mcl.initialize_logging(mcl.LOGGING_NONE)

    def test_log_v(self):
        mcl.mc_log_level = mcl.LOGGING_V
        self.logger.log_v('test message v %s %s', 'aaa', 1)

    def test_log_vv(self):
        mcl.mc_log_level = mcl.LOGGING_VV
        self.logger.log_vv('test message vv %s %s', 'aa\r\naa', 2)

    def test_log_vvv(self):
        mcl.mc_log_level = mcl.LOGGING_VVV
        self.logger.log_vvv('test message vvv %s %s', 'a\ra\na', 3)

if __name__ == "__main__":
    unittest.main()
