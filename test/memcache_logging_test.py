#!/usr/local/bin/python
"""
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
