import memory_cache
import pyev

VERSION = "0.1"

class ProtocolStats(memory_cache.MemcachedStats):
    def __init__(self):
        super(ProtocolStats, self).__init__()

        self.version = VERSION
        self.bytes_read = 0
        self.bytes_written = 0

    def read_bytes(self, count):
        self.bytes_read += count

    def write_bytes(self, count):
        self.bytes_written += count

    def dump(self, command):
        ret_super = super(ProtocolStats, self).dump(command)
        ret = [('bytes_read', self.bytes_read),
               ('bytes_written', self.bytes_written),
               ('version', self.version)]
        ret_super.extend(ret)
        return ret_super

class ProtocolException(Exception):
    def __init__(self, msg):
        self.msg = msg

class QuitException(Exception):
    def __init__(self, msg):
        self.msg = msg

class MCProtocol(object):
    STATE_R_SEARCH = 0
    STATE_N_SEARCH = 1
    STATE_BODY = 2
    STATE_DONE = 3

    STATS_TYPES = ["settings", "items", "sizes", "slabs"]

    def __init__(self, stats, mc):
        self.state = self.STATE_R_SEARCH
        self.stats = stats
        self.memcached = mc
        self.buf = ""
        self.noreply = False

    def got_input(self, buf):
        """ return true when got a full command """
        self.stats.read_bytes(len(buf))
        while buf:
            if self.state == self.STATE_R_SEARCH:
                delimiter = buf.find('\r')
                if delimiter > -1:
                    command = self.buf + buf[:delimiter]
                    buf = buf[delimiter+1:]
                    self.state = self.STATE_N_SEARCH
                    self.has_body = self.parse_command(command) > 0
                else:
                    self.buf += buf
                    buf = ""
            elif self.state == self.STATE_N_SEARCH:
                if buf[0] != '\n':
                    raise ProtocolException('Malformed request')
                else:
                    buf = buf[1:]
                    self.buf = ""
                    if self.has_body:
                        self.state = self.STATE_BODY
                    else:
                        self.state = self.STATE_DONE
            elif self.state == self.STATE_BODY:
                self.buf += buf
                buf = ""
                if len(self.buf) > int(self.bytes) + 2:
                    raise ProtocolException('Malformed request')
                elif len(self.buf) == int(self.bytes) + 2:
                    if self.buf[-2] != '\r' or self.buf[-1] != '\n':
                        raise ProtocolException('Malformed request')
                    else:
                        self.buf = self.buf[:int(self.bytes)]
                        self.state = self.STATE_DONE

        if self.state == self.STATE_DONE:
            self.execute_command()
            return True
        else:
            return False

    def get_output(self):
        """ give output and reset buffer """
        if self.noreply:
            retval = ""
        else:
            retval = self.buf
        self.buf = ""
        self.noreply = False
        self.state = self.STATE_R_SEARCH
        self.stats.write_bytes(len(retval))
        return retval

    def parse_command(self, command_string):
        """ return size of payload in bytes """
        command_info = command_string.split()
        self.command = command_info[0]

        if self.command in ('set', 'add', 'replace', 'prepend', 'append'):
            if len(command_info) < 5:
                raise ProtocolException('CLIENT_ERROR not enough arguments\r\n')
            else:
                self.key = command_info[1]
                self.flags = command_info[2]
                self.exptime = command_info[3]
                self.bytes = command_info[4]
                self.noreply = len(command_info) == 6 and command_info[5] == 'noreply'

                if len(self.flags) > 1:
                    raise ProtocolException("CLIENT_ERROR bad flags\r\n")

                if (not self.flags.isdigit() or not self.exptime.isdigit() or
                    not self.bytes.isdigit()):
                    raise ProtocolException("CLIENT_ERROR bad argument\r\n")

                return self.bytes

        elif self.command == 'cas':
            if len(command_info) < 6:
                raise ProtocolException('CLIENT_ERROR not enough arguments\r\n')
            else:
                self.key = command_info[1]
                self.flags = command_info[2]
                self.exptime = command_info[3]
                self.bytes = command_info[4]
                self.casunique = command_info[5]
                self.noreply = len(command_info) == 7 and command_info[6] == 'noreply'

                if len(self.flags) > 1:
                    raise ProtocolException("CLIENT_ERROR bad flags\r\n")

                if (not self.flags.isdigit() or not self.exptime.isdigit() or
                    not self.bytes.isdigit() or not self.casunique.isdigit()):
                    raise ProtocolException("CLIENT_ERROR bad argument\r\n")

                return self.bytes

        elif self.command in ('get', 'gets'):
            if len(command_info) < 2:
                raise ProtocolException('CLIENT_ERROR not enough arguments\r\n')
            else:
                self.keys = command_info[1:]

        elif self.command == 'delete':
            if len(command_info) < 2:
                raise ProtocolException('CLIENT_ERROR not enough arguments\r\n')
            else:
                self.key = command_info[1]
                self.noreply = len(command_info) == 3 and command_info[2] == 'noreply'

        elif self.command in ('incr', 'decr'):
            if len(command_info) < 3:
                raise ProtocolException('CLIENT_ERROR not enough arguments\r\n')
            else:
                self.key = command_info[1]
                self.value = command_info[2]
                self.noreply = len(command_info) == 4 and command_info[3] == 'noreply'

                if not self.value.isdigit():
                    raise ProtocolException("CLIENT_ERROR bad argument\r\n")

        elif self.command == 'stats':
            if len(command_info) > 1:
                self.sub_command = command_info[1]

                if self.sub_command not in self.STATS_TYPES:
                    raise ProtocolException("CLIENT_ERROR invalid statistic requested\r\n")
            else:
                self.sub_command = ""

        elif self.command == 'flush_all':
            if len(command_info) > 1:
                if command_info[1] == 'noreply':
                    self.delay = 0
                    self.noreply = True
                else:
                    self.delay = command_info[1]
                    
                    if not self.delay.isdigit():
                        raise ProtocolException("CLIENT_ERROR bad argument\r\n")

                    self.noreply = len(command_info) == 3 and command_info[2] == 'noreply'
            else:
                self.delay = 0
                self.noreply = False

        elif self.command == 'version':
            pass

        elif self.command == 'verbosity':
            pass

        elif self.command == 'quit':
            pass

        else:
            raise ProtocolException("ERROR\r\n")

        return 0

    def execute_command(self):
        if self.command == 'set':
            self.memcached.set(self.key, self.flags, self.exptime, self.buf)
            self.buf = "STORED\r\n"

        elif self.command == 'cas':
            ret = self.memcached.cas(self.key, self.flags, self.exptime, self.casunique, self.buf)
            if ret == self.memcached.STORED:
                self.buf = "STORED\r\n"
            elif ret == self.memcached.NOT_FOUND:
                self.buf = "NOT_FOUND\r\n"
            elif ret == self.memcached.EXISTS:
                self.buf = "EXISTS\r\n"

        elif self.command == 'add':
            ret = self.memcached.add(self.key, self.flags, self.exptime, self.buf)
            if ret == self.memcached.STORED:
                self.buf = "STORED\r\n"
            elif ret == self.memcached.NOT_STORED:
                self.buf = "NOT_STORED\r\n"

        elif self.command == 'replace':
            ret = self.memcached.replace(self.key, self.flags, self.exptime, self.buf)
            if ret == self.memcached.STORED:
                self.buf = "STORED\r\n"
            elif ret == self.memcached.NOT_STORED:
                self.buf = "NOT_STORED\r\n"

        elif self.command == 'prepend':
            ret = self.memcached.prepend(self.key, self.flags, self.exptime, self.buf)
            if ret == self.memcached.STORED:
                self.buf = "STORED\r\n"
            elif ret == self.memcached.NOT_STORED:
                self.buf = "NOT_STORED\r\n"

        elif self.command == 'append':
            ret = self.memcached.append(self.key, self.flags, self.exptime, self.buf)
            if ret == self.memcached.STORED:
                self.buf = "STORED\r\n"
            elif ret == self.memcached.NOT_STORED:
                self.buf = "NOT_STORED\r\n"

        elif self.command == 'get':
            items = self.memcached.get(self.keys)
            self.buf = ""
            for key, value, flags in items:
                self.buf += "VALUE %s %s %s\r\n" % (key, flags, len(value))
                self.buf += value + "\r\n"
            self.buf += "END\r\n"

        elif self.command == 'gets':
            items = self.memcached.gets(self.keys)
            self.buf = ""
            for key, value, flags, casunique in items:
                self.buf += "VALUE %s %s %s %s\r\n" % (key, flags, len(value), casunique)
                self.buf += value + "\r\n"
            self.buf += "END\r\n"

        elif self.command == 'delete':
            ret = self.memcached.delete(self.key)
            if ret == self.memcached.DELETED:
                self.buf = "DELETED\r\n"
            elif ret == self.memcached.NOT_FOUND:
                self.buf = "NOT_FOUND\r\n"

        elif self.command == 'incr':
            ret, new_val = self.memcached.increment(self.key, self.value)
            if ret == self.memcached.NOT_NUMBER:
                self.buf = "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n"
            elif ret == self.memcached.NOT_FOUND:
                self.buf = "NOT_FOUND\r\n"
            elif ret == self.memcached.STORED:
                self.buf = "%s\r\n" % new_val

        elif self.command == 'decr':
            ret, new_val = self.memcached.decrement(self.key, self.value)
            if ret == self.memcached.NOT_NUMBER:
                self.buf = "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n"
            elif ret == self.memcached.NOT_FOUND:
                self.buf = "NOT_FOUND\r\n"
            elif ret == self.memcached.STORED:
                self.buf = "%s\r\n" % new_val

        elif self.command == 'stats':
            items = self.memcached.stats(self.sub_command)
            commands = ["STAT %s %s\r\n" % (name, value) for name, value in items]
            commands.append("END\r\n")
            self.buf = "".join(commands)

        elif self.command == 'flush_all':
            self.memcached.flush(self.delay)
            self.buf = "OK\r\n"

        elif self.command == 'quit':
            raise QuitException("quit command received")

        elif self.command == 'version':
            self.buf = "VERSION %s\r\n" % VERSION
