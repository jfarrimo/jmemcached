"""
Handle connections from other people and do the right thing with them.

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
import errno
import os
import pyev
import socket
import signal
import time
import weakref

import memcache_logging as mc_log
import memcache_protocol
import memcache_protocol_execute
import memcache_protocol_parse
import memory_cache

STOPSIGNALS = (signal.SIGINT, signal.SIGTERM)
NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

class ConnectionStats(memcache_protocol.ProtocolStats):
    """ collect statistics for a connection """

    def __init__(self):
        super(ConnectionStats, self).__init__()

        self.start_time = int(time.time())

        self.curr_connections = 0
        self.total_connections = 0
        self.connection_structures = 0

    def connect(self):
        """ comeone has connected """
        self.curr_connections += 1
        self.total_connections += 1
        self.connection_structures += 1

    def disconnect(self):
        """ someone has disconnected """
        self.curr_connections -= 1
        self.connection_structures -= 1

    def dump(self, command):
        """ dump the collected statistics """
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
    """ 
    wrapper for a socket

    this is separate from MemcachedConnection since it's easily
    unit testable and the other isn't
    """
    CONTINUE = 0
    ERROR = 1
    FINISHED = 2
    OK = 3
    QUIT = 4

    def __init__(self, sock, address, stats, cache):
        self.logger = mc_log.MemcachedLogger(address)
        self.protocol = memcache_protocol.MCProtocol(stats, cache, address)
        self.reply = ""
        self.sock = sock
        self.sock.setblocking(0)
        self.stats = stats
        self.stats.connect()
        self.logger.log_v("socket ready")

    def handle_error(self, msg, exc_info=True):
        """ log the error and close the connection """
        self.logger.log_v("socket closing --> %s", msg,
                          exc_info=exc_info)
        self.close()

    def close(self):
        """ close the socket and record it """
        self.sock.close()
        self.stats.disconnect()
        self.logger.log_v("socket closed")

    def handle_read(self):
        """ 
        read data from the socket

        will probably call this multiple times per message if the message
        is larger than the tcp packet size

        if there is a reply, buffer it for later sending
        """
        try:
            buf = self.sock.recv(4096)
        except socket.error as err:
            if err.args[0] not in NONBLOCKING:
                self.handle_error(
                    "socket error reading from {0}".format(self.sock))
                return self.ERROR

        if buf:
            try:
                self.reply = self.protocol.got_input(buf)
                if self.reply is not None:
                    return self.FINISHED
            except memcache_protocol_parse.ProtocolException, err:
                self.reply = err.msg
                return self.FINISHED
            except memcache_protocol_execute.QuitException:
                self.close()
                return self.QUIT
        else:
            self.handle_error("socket connection closed by peer", False)
            return self.ERROR
        return self.CONTINUE

    def handle_write(self):
        """ 
        write data to the socket 

        data is save from a previous call to handle_read
        """
        try:
            sent = self.sock.send(self.reply)
        except socket.error as err:
            if err.args[0] not in NONBLOCKING:
                self.handle_error(
                    "socket error writing to {0}".format(self.sock))
                return self.ERROR
        else:
            self.reply = self.reply[sent:]
            if not self.reply:
                return self.FINISHED
        return self.OK

# we don't do coverage for this since it's a pain to do a unit
# test for... much easier to test by running the cache and
# hitting it a bit
class MemcachedConnection(object): # pragma: no cover
    """ connection from a client to this server """
    # pylint: disable=R0913
    def __init__(self, sock, address, loop, stats, cache):
        # pylint: disable=W0212
        self.logger = mc_log.MemcachedLogger(address)
        self.socket = MemcachedSocket(sock, address, stats, cache)
        self.watcher = pyev.Io(sock._sock, pyev.EV_READ, loop, self.io_cb)
        self.watcher.start()
        self.logger.log_v("connection ready")
    # pylint: enable=R0913,W0212

    def reset(self, events):
        """ change from read to write or vice-versa """
        self.watcher.stop()
        self.watcher.set(self.socket.sock, events)
        self.watcher.start()

    def io_cb(self, watcher, revents):
        """ callback for when the socket is ready to read/write io """
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
        """ shut it down """
        self.watcher.stop()
        self.watcher = None
        self.logger.log_v("connection closed")

# no coverage, same reason as above
class Server(object): # pragma: no cover
    """ handle incoming connections """
    def __init__(self, interface="", tcp_port=11211, max_bytes=1024*1024*1024):
        self.loop = pyev.default_loop()
        self.watchers = [pyev.Signal(sig, self.loop, self.signal_cb)
                         for sig in STOPSIGNALS]

        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        address = (interface, tcp_port)
        self.sock.bind(address)
        self.sock.setblocking(0)
        # pylint: disable=W0212
        self.watchers.append(
            pyev.Io(self.sock._sock, pyev.EV_READ, self.loop, self.io_cb))
        # pylint: enable=W0212

        self.logger = mc_log.MemcachedLogger(address)
        self.conns = weakref.WeakValueDictionary()
        self.stats = ConnectionStats()
        self.cache = memory_cache.Memcached(self.stats, max_bytes=max_bytes)

    def handle_error(self, msg, exc_info=True):
        """ log it and shut down """
        self.logger.log_v("server stopping --> %s", msg,
                          exc_info=exc_info)
        self.stop()

    def signal_cb(self, watcher, revents):
        """ we got signals, all of which mean to stop """
        self.stop()

    def io_cb(self, watcher, revents):
        """ 
        we got some activity on our port

        always someone trying to connect, so setup a connection
        """
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
                        sock, address, self.loop, self.stats, self.cache)
        except Exception: # pylint: disable=W0703
            self.handle_error("server error accepting a connection")

    def start(self):
        """ start the listening """
        self.sock.listen(socket.SOMAXCONN)
        for watcher in self.watchers:
            watcher.start()
        self.logger.log_v("server started")
        self.loop.start()

    def stop(self):
        """ stop listening """
        self.loop.stop(pyev.EVBREAK_ALL)
        self.sock.close()
        while self.watchers:
            self.watchers.pop().stop()
        for conn in self.conns.values():
            conn.close()
        self.logger.log_v("server stopped")
