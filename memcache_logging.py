"""
Logging the memcached way.

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
import logging

LOGGING_VVV = 10
LOGGING_VV = 20
LOGGING_V = 30
LOGGING_NONE = 40

LOGGING_DESCRIPTIONS = {
    10: 'EXTREMELY VERBOSE',
    20: 'VERY VERBOSE',
    30: 'VERBOSE',
    40: 'NONE'}

mc_log_level = LOGGING_NONE # pylint: disable=C0103

def initialize_logging(level):
    """
    setup logging the way we want it
    """
    global mc_log_level # pylint: disable=W0603
    mc_log_level = level

    logging.addLevelName(
        LOGGING_VVV, 
        LOGGING_DESCRIPTIONS[LOGGING_VVV])
    logging.addLevelName(
        LOGGING_VV, 
        LOGGING_DESCRIPTIONS[LOGGING_VV])
    logging.addLevelName(
        LOGGING_V, 
        LOGGING_DESCRIPTIONS[LOGGING_V])
    logging.addLevelName(
        LOGGING_NONE, 
        LOGGING_DESCRIPTIONS[LOGGING_NONE])

    logging.basicConfig(level=level,
                        format="%(asctime)s - %(message)s")

def _msg_replace(msg):
    """
    replace \r\n with string representation
    """
    if isinstance(msg, basestring):
        return msg.replace('\r', '\\r').replace('\n', '\\n')
    else:
        return msg

class MemcachedLogger(object):
    """
    address-aware logger
    """
    def __init__(self, address):
        self.address = address

    def _log_it(self, level, msg, *args, **kwargs):
        """
        add address to message, translate \r\n,  and log it
        """
        if mc_log_level <= level:
            # pylint: disable=W0142
            new_args = [_msg_replace(arg) for arg in args]
            new_args.insert(0, self.address)
            msg = '%s - ' + msg
            logging.log(level, msg, *new_args, **kwargs)
            # pylint: enable=W0142

    def log_v(self, msg, *args, **kwargs):
        """
        verbose log
        """
        self._log_it(LOGGING_V, msg, *args, **kwargs)

    def log_vv(self, msg, *args, **kwargs):
        """
        very verbose log
        """
        self._log_it(LOGGING_VV, msg, *args, **kwargs)

    def log_vvv(self, msg, *args, **kwargs):
        """
        extremely verbose log
        """
        self._log_it(LOGGING_VVV, msg, *args, **kwargs)
