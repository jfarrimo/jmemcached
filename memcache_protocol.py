"""
Memcached protocol handling.

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
import memcache_logging as mc_log
import memcache_protocol_execute as mp_execute
import memcache_protocol_parse as mp_parse
import memory_cache

class ProtocolStats(memory_cache.MemcachedStats):
    """ protocol-related statistics """
    def __init__(self):
        super(ProtocolStats, self).__init__()

        self.version = mp_execute.VERSION
        self.bytes_read = 0
        self.bytes_written = 0

    def read_bytes(self, count):
        """ bytes were read, presumably from socket """
        self.bytes_read += count

    def write_bytes(self, count):
        """ bytes ready to be written """
        self.bytes_written += count

    def dump(self, command):
        """ dump our values for output """
        ret_super = super(ProtocolStats, self).dump(command)
        ret = [('bytes_read', self.bytes_read),
               ('bytes_written', self.bytes_written),
               ('version', self.version)]
        ret_super.extend(ret)
        return ret_super

class MCProtocol(object): # pylint: disable=R0903
    """
    State machine to handle the memcached protocol, taking the 
    input, processing it, and returning the output.
    """
    STATE_R_SEARCH = 0
    STATE_N_SEARCH = 1
    STATE_BODY = 2
    STATE_DONE = 3

    def __init__(self, stats, memcached, address):
        self.logger = mc_log.MemcachedLogger(address)
        self.logger.log_vvv("entering R_SEARCH state")
        self.state = self.STATE_R_SEARCH
        self.stats = stats
        self.memcached = memcached
        self.buf = ""
        self.command = None

    def _state_r_search(self, buf):
        """
        search for \r part or \r\n that ends command part of command string
        """
        delimiter = buf.find('\r')
        if delimiter > -1:
            command_string = self.buf + buf[:delimiter]
            buf = buf[delimiter+1:] # skip the \r
            self.logger.log_vv("command string = '%s'", command_string)
            self.logger.log_vvv("entering N_SEARCH state")
            self.state = self.STATE_N_SEARCH
            self.command = mp_parse.parse_command(command_string)
        else:
            self.buf += buf
            buf = ""
        return buf

    def _state_n_search(self, buf):
        """ 
        search for \n part of \r\n ending command string

        have to do this in separate state since \r and \n could be
        split up by TCP
        """
        if buf[0] != '\n':
            raise mp_parse.ProtocolException('Malformed request')
        else:
            buf = buf[1:]
            self.buf = ""
            if int(self.command.bytes) > 0:
                self.logger.log_vvv("entering BODY_SEARCH state")
                self.state = self.STATE_BODY
            else:
                self.logger.log_vvv("entering DONE state")
                self.state = self.STATE_DONE
        return buf

    def _state_body(self, buf):
        """
        get the body of the request, the value portion
        """
        self.buf += buf
        buf = ""
        if len(self.buf) > int(self.command.bytes) + 2:
            raise mp_parse.ProtocolException('Malformed request')
        elif len(self.buf) == int(self.command.bytes) + 2:
            if self.buf[-2] != '\r' or self.buf[-1] != '\n':
                raise mp_parse.ProtocolException('Malformed request')
            else:
                self.buf = self.buf[:int(self.command.bytes)]
                self.logger.log_vv("body = '%s'", self.buf)
                self.logger.log_vvv("entering DONE state")
                self.state = self.STATE_DONE
        return buf

    def got_input(self, buf):
        """ 
        state machine for parsing a command from TCP input

        return output when got full command 
        """
        self.stats.read_bytes(len(buf))
        while buf:
            if self.state == self.STATE_R_SEARCH:
                buf = self._state_r_search(buf)
            elif self.state == self.STATE_N_SEARCH:
                buf = self._state_n_search(buf)
            elif self.state == self.STATE_BODY:
                buf = self._state_body(buf)

        if self.state == self.STATE_DONE:
            retval = mp_execute.execute_command(
                self.command, self.memcached, self.buf)
            self.buf = ""
            self.state = self.STATE_R_SEARCH
            self.stats.write_bytes(len(retval))
            self.logger.log_vv("response = '%s'", retval)
            self.logger.log_vvv("entering R_SEARCH state")
            return retval
        else:
            return None
