import errno
import logging
import os
import pyev
import socket
import signal
import time
import weakref

import memcache_protocol
import memory_cache

logging.basicConfig(level=logging.DEBUG)

STOPSIGNALS = (signal.SIGINT, signal.SIGTERM)
NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

class ConnectionStats(memcache_protocol.ProtocolStats):
    def __init__(self):
        super(ConnectionStats, self).__init__()

        self.start_time = int(time.time())

        self.curr_connections = 0
        self.total_connections = 0
        self.connection_structures = 0

    def connect(self):
        self.curr_connections += 1
        self.total_connections += 1
        self.connection_structures += 1

    def disconnect(self):
        self.curr_connections -= 1
        self.connection_structures -= 1

    def dump(self, command):
        ret_super = super(ConnectionStats, self).dump(command)
        now_time = int(time.time())
        rusage_user, rusage_system, _, _, _ = os.times()
        ret = [('pid', os.getpid()),
               ('uptime', now_time - self.start_time),
               ('time',  now_time),
               ('pointer_size', 64),
               ('rusage_user', rusage_user),
               ('rusage_system', rusage_system),
               ('curr_connections', self.curr_connections),
               ('total_connections', self.total_connections),
               ('connection_structures', self.connection_structures),
               ('threads', 1),
               ('conn_yields', 0)]
        ret_super.extend(ret)
        return ret_super

class MemcachedSocket(object):
    OK = 0
    FINISHED = 1
    QUIT = 2
    ERROR = 3

    def __init__(self, sock, address, stats, mc):
        self.protocol = memcache_protocol.MCProtocol(stats, mc)
        self.buf = ""
        self.sock = sock
        self.address = address
        self.sock.setblocking(0)
        self.stats = stats
        self.stats.connect()
        logging.debug("{0}: ready".format(self))

    def handle_error(self, msg, level=logging.ERROR, exc_info=True):
        logging.log(level, "{0}: {1} --> closing".format(self, msg),
                    exc_info=exc_info)
        self.close()

    def close(self):
        self.sock.close()
        self.stats.disconnect()
        logging.debug("{0}: closed".format(self))

    def handle_read(self):
        try:
            buf = self.sock.recv(4096)
        except socket.error as err:
            if err.args[0] not in NONBLOCKING:
                self.handle_error("error reading from {0}".format(self.sock))
                return self.ERROR

        if buf:
            try:
                if self.protocol.got_input(buf):
                    self.buf = self.protocol.get_output()
                    return self.FINISHED
            except memcache_protocol.ProtocolException, err:
                self.buf = err.msg
                return self.FINISHED
            except memcache_protocol.QuitException:
                self.close()
                return self.QUIT
        else:
            self.handle_error("connection closed by peer", logging.DEBUG, False)
            return self.ERROR

    def handle_write(self):
        try:
            sent = self.sock.send(self.buf)
        except socket.error as err:
            if err.args[0] not in NONBLOCKING:
                self.handle_error("error writing to {0}".format(self.sock))
                return self.ERROR
        else:
            self.buf = self.buf[sent:]
            if not self.buf:
                return self.FINISHED
        return self.OK

class MemcachedConnection(object):
    def __init__(self, sock, address, loop, stats, mc):
        self.socket = MemcachedSocket(sock, address, stats, mc)
        self.watcher = pyev.Io(sock._sock, pyev.EV_READ, loop, self.io_cb)
        self.watcher.start()
        logging.debug("{0}: ready".format(self))

    def reset(self, events):
        self.watcher.stop()
        self.watcher.set(self.socket.sock, events)
        self.watcher.start()

    def io_cb(self, watcher, revents):
        if revents & pyev.EV_READ:
            ret = self.socket.handle_read()
            if ret == self.socket.FINISHED:
                self.reset(pyev.EV_WRITE)
            elif ret == self.socket.ERROR or ret == self.socket.QUIT:
                self.close()
        else:
            ret = self.socket.handle_write()
            if ret == self.socket.FINISHED:
                self.reset(pyev.EV_READ)
            elif ret == self.socket.ERROR:
                self.close()

    def close(self):
        self.watcher.stop()
        self.watcher = None
        logging.debug("{0}: closed".format(self))

class Server(object):
    def __init__(self, address=("127.0.0.1", 11212)):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(address)
        self.sock.setblocking(0)
        self.address = self.sock.getsockname()
        self.loop = pyev.default_loop()
        self.watchers = [pyev.Signal(sig, self.loop, self.signal_cb)
                         for sig in STOPSIGNALS]
        self.watchers.append(
            pyev.Io(self.sock._sock, pyev.EV_READ, self.loop, self.io_cb))
        self.conns = weakref.WeakValueDictionary()
        self.stats = ConnectionStats()
        self.mc = memory_cache.Memcached(self.stats, max_bytes=1024*1024*1024)

    def handle_error(self, msg, level=logging.ERROR, exc_info=True):
        logging.log(level, "{0}: {1} --> stopping".format(self, msg),
                    exc_info=exc_info)
        self.stop()

    def signal_cb(self, watcher, revents):
        self.stop()

    def io_cb(self, watcher, revents):
        try:
            while True:
                try:
                    sock, address = self.sock.accept()
                except socket.error as err:
                    if err.args[0] in NONBLOCKING:
                        break
                    else:
                        raise
                else:
                    self.conns[address] = MemcachedConnection(
                        sock, address, self.loop, self.stats, self.mc)
        except Exception:
            self.handle_error("error accepting a connection")

    def start(self):
        self.sock.listen(socket.SOMAXCONN)
        for watcher in self.watchers:
            watcher.start()
        logging.debug("{0}: started on {0.address}".format(self))
        self.loop.start()

    def stop(self):
        self.loop.stop(pyev.EVBREAK_ALL)
        self.sock.close()
        while self.watchers:
            self.watchers.pop().stop()
        for conn in self.conns.values():
            conn.close()
        logging.debug("{0}: stopped".format(self))
