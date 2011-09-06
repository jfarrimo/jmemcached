"""
Memcached protocol handling.
"""
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

class MCProtocol(object):
    """
    Handle the memcached protocol, taking the input, processing it,
    and returning the output.
    """
    STATE_R_SEARCH = 0
    STATE_N_SEARCH = 1
    STATE_BODY = 2
    STATE_DONE = 3

    def __init__(self, stats, memcached):
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
            buf = buf[delimiter+1:]
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
                self.state = self.STATE_BODY
            else:
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
                self.state = self.STATE_DONE
        return buf

    def got_input(self, buf):
        """ 
        state machine for parsing a command from tcp input

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
            return retval
        else:
            return None
