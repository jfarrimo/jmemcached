VERSION = "0.1"

class ExecuteException(Exception):
    def __init__(self, msg):
        super(ExecuteException, self).__init__(self)
        self.msg = msg

class QuitException(Exception):
    def __init__(self, msg):
        super(QuitException, self).__init__(self)
        self.msg = msg

COMMANDS = {}

def set_it(command, memcached, buf):
    memcached.set(command.key, command.flags, command.exptime, buf)
    return "STORED\r\n"

COMMANDS['set'] = set_it

def cas(command, memcached, buf):
    ret = memcached.cas(command.key, command.flags, 
                        command.exptime, command.casunique, buf)
    if ret == memcached.STORED:
        return "STORED\r\n"
    elif ret == memcached.NOT_FOUND:
        return "NOT_FOUND\r\n"
    elif ret == memcached.EXISTS:
        return "EXISTS\r\n"

COMMANDS['cas'] = cas

def add(command, memcached, buf):
    ret = memcached.add(command.key, command.flags, command.exptime, buf)
    if ret == memcached.STORED:
        return "STORED\r\n"
    elif ret == memcached.NOT_STORED:
        return "NOT_STORED\r\n"

COMMANDS['add'] = add

def replace(command, memcached, buf):
    ret = memcached.replace(command.key, command.flags, command.exptime, buf)
    if ret == memcached.STORED:
        return "STORED\r\n"
    elif ret == memcached.NOT_STORED:
        return "NOT_STORED\r\n"

COMMANDS['replace'] = replace

def prepend(command, memcached, buf):
    ret = memcached.prepend(command.key, command.flags, command.exptime, buf)
    if ret == memcached.STORED:
        return "STORED\r\n"
    elif ret == memcached.NOT_STORED:
        return "NOT_STORED\r\n"

COMMANDS['prepend'] = prepend

def append(command, memcached, buf):
    ret = memcached.append(command.key, command.flags, command.exptime, buf)
    if ret == memcached.STORED:
        return "STORED\r\n"
    elif ret == memcached.NOT_STORED:
        return "NOT_STORED\r\n"

COMMANDS['append'] = append

def get(command, memcached, _):
    items = memcached.get(command.keys)
    ret_buf = ""
    for key, value, flags in items:
        ret_buf += "VALUE %s %s %s\r\n" % (key, flags, len(value))
        ret_buf += value + "\r\n"
    ret_buf += "END\r\n"
    return ret_buf

COMMANDS['get'] = get

def gets(command, memcached, _):
    items = memcached.gets(command.keys)
    ret_buf = ""
    for key, value, flags, casunique in items:
        ret_buf += "VALUE %s %s %s %s\r\n" % (key, flags, 
                                              len(value), casunique)
        ret_buf += value + "\r\n"
    ret_buf += "END\r\n"
    return ret_buf

COMMANDS['gets'] = gets

def delete(command, memcached, _):
    ret = memcached.delete(command.key)
    if ret == memcached.DELETED:
        return "DELETED\r\n"
    elif ret == memcached.NOT_FOUND:
        return "NOT_FOUND\r\n"

COMMANDS['delete'] = delete

def incr(command, memcached, _):
    ret, new_val = memcached.increment(command.key, command.value)
    if ret == memcached.NOT_NUMBER:
        return "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n"
    elif ret == memcached.NOT_FOUND:
        return "NOT_FOUND\r\n"
    elif ret == memcached.STORED:
        return "%s\r\n" % new_val

COMMANDS['incr'] = incr

def decr(command, memcached, _):
    ret, new_val = memcached.decrement(command.key, command.value)
    if ret == memcached.NOT_NUMBER:
        return "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n"
    elif ret == memcached.NOT_FOUND:
        return "NOT_FOUND\r\n"
    elif ret == memcached.STORED:
        return "%s\r\n" % new_val

COMMANDS['decr'] = decr

def stats(command, memcached, _):
    items = memcached.stats(command.stats_command)
    commands = ["STAT %s %s\r\n" % (name, value) 
                for name, value in items]
    commands.append("END\r\n")
    return "".join(commands)

COMMANDS['stats'] = stats

def flush_all(command, memcached, _):
    memcached.flush(command.delay)
    return "OK\r\n"

COMMANDS['flush_all'] = flush_all

def quit_it(_, __, ___):
    raise QuitException("quit command received")

COMMANDS['quit'] = quit_it

def version(_, __, ___):
    return "VERSION %s\r\n" % VERSION

COMMANDS['version'] = version

def execute_command(command, memcached, buf):
    if command.command not in COMMANDS:
        raise ExecuteException("ERROR bad command")
    return command.reply(COMMANDS[command.command](command, memcached, buf))
