"""
Parse a memcached command string.  Return a command object.
"""

class ProtocolException(Exception):
    """ Exeption thrown for commands that don't conform to the protocol """
    def __init__(self, msg):
        super(ProtocolException, self).__init__(self)
        self.msg = msg

class MCCommand(object): # pylint: disable=R0902,R0903
    """ parsed memcached command """
    def __init__(self, # pylint: disable=R0913
                 command = '',
                 key = '',
                 flags = '0',
                 exptime = '0',
                 in_bytes = '0',
                 noreply = False,
                 casunique = '',
                 keys = None,
                 value = '0',
                 delay = '0',
                 stats_command = ''):

        if len(flags) > 1:
            raise ProtocolException("CLIENT_ERROR bad flags\r\n")

        if (not flags.isdigit() or not exptime.isdigit() or
            not in_bytes.isdigit() or not delay.isdigit() or
            not value.isdigit()):
            raise ProtocolException("CLIENT_ERROR bad argument\r\n")

        if (stats_command and 
            stats_command not in ["settings", "items", "sizes", "slabs"]):
            raise ProtocolException(
                "CLIENT_ERROR invalid statistic requested\r\n")

        self.command = command
        self.key = key
        self.flags = flags
        self.exptime = exptime
        self.bytes = in_bytes
        self.noreply = noreply
        self.casunique = casunique
        self.keys = keys
        self.delay = delay
        self.stats_command = stats_command
        self.value = value

    def reply(self, reply_val):
        """ nothing if noreply set, otherwise command reply """
        if self.noreply:
            return ""
        else:
            return reply_val

def check_command_length(command_info, length):
    """ make sure there are enough command arguments """
    if len(command_info) < length:
        raise ProtocolException('CLIENT_ERROR not enough arguments\r\n')

COMMANDS = {}

def set_et_al(command_info):
    """ parse set, add, replace, prepend and append commands """
    check_command_length(command_info, 5)
    return MCCommand(command = command_info[0],
                     key = command_info[1],
                     flags = command_info[2],
                     exptime = command_info[3],
                     in_bytes = command_info[4],
                     noreply = (len(command_info) == 6 and 
                                command_info[5] == 'noreply'))

COMMANDS['set'] = set_et_al
COMMANDS['add'] = set_et_al
COMMANDS['replace'] = set_et_al
COMMANDS['prepend'] = set_et_al
COMMANDS['append'] = set_et_al

def cas(command_info):
    """ parse cas command """
    check_command_length(command_info, 6)
    return MCCommand(command = command_info[0],
                     key = command_info[1],
                     flags = command_info[2],
                     exptime = command_info[3],
                     in_bytes = command_info[4],
                     casunique = command_info[5],
                     noreply = (len(command_info) == 7 and 
                                command_info[6] == 'noreply'))

COMMANDS['cas'] = cas

def get(command_info):
    """ parse get and gets commands """
    check_command_length(command_info, 2)
    return MCCommand(command = command_info[0],
                     keys = command_info[1:])

COMMANDS['get'] = get
COMMANDS['gets'] = get

def delete(command_info):
    """ parse delete command """
    check_command_length(command_info, 2)
    return MCCommand(command = command_info[0],
                     key = command_info[1],
                     noreply = (len(command_info) == 3 and 
                                command_info[2] == 'noreply'))

COMMANDS['delete'] = delete

def incr(command_info):
    """ parse incr and decr commands """
    check_command_length(command_info, 3)
    return MCCommand(command = command_info[0],
                     key = command_info[1],
                     value = command_info[2],
                     noreply = (len(command_info) == 4 and 
                                command_info[3] == 'noreply'))

COMMANDS['incr'] = incr
COMMANDS['decr'] = incr

def stats(command_info):
    """ parse stats commands """
    if len(command_info) > 1:
        return MCCommand(command = command_info[0],
                         stats_command = command_info[1])
    else:
        return MCCommand(command = command_info[0])

COMMANDS['stats'] = stats

def flush_all(command_info):
    """ parse flush_all command """
    if len(command_info) > 1:
        if command_info[1] == 'noreply':
            return MCCommand(command = command_info[0],
                             noreply = True)
        else:
            return MCCommand(command = command_info[0],
                             delay = command_info[1],
                             noreply = (len(command_info) == 3 and 
                                        command_info[2] == 'noreply'))
    else:
        return MCCommand(command = command_info[0])

COMMANDS['flush_all'] = flush_all

def simple(command_info):
    """ parse version, verbosity, and quit commands """
    return MCCommand(command = command_info[0])

COMMANDS['version'] = simple
COMMANDS['verbosity'] = simple
COMMANDS['quit'] = simple

def parse_command(command_string):
    """ parse all commands """
    command_info = command_string.split()
    if command_info[0] not in COMMANDS:
        raise ProtocolException("ERROR\r\n")
    else:
        return COMMANDS[command_info[0]](command_info)
