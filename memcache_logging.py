"""
logging the memcached way
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
