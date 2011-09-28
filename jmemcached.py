"""
Setup and start jmemcached.

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

import daemon
import pwd
import optparse
import os

from lockfile.pidlockfile import PIDLockFile

import memcache_logging as mc_log
import memcache_connection

def parse_command_line():
    """ parse the command line """

    parser = optparse.OptionParser()

    parser.add_option("-p", "--tcp-port", dest="tcp_port", type="int", 
                      default=11211, metavar="PORT",
                      help="TCP port number to listen on (default: %default)")
    parser.add_option("-I", "--interface", dest="interface", 
                      default="", metavar="INTERFACE",
                      help="interface to listen on (default: INADDR_ANY, "
                      "all addresses)")
    parser.add_option("-m", "--max-memory", dest="max_memory", type="int", 
                      default=64, metavar="MB",
                      help="max memory to use for items in megabytes "
                      "(default: %default MB)")
    parser.add_option("-d", "--daemonize", dest="daemonize", 
                      action="store_true", default=False,
                      help="run as a daemon")
    parser.add_option("-u", "--username", dest="username", metavar="USERNAME",
                      default="",
                      help="assume identity of <username> (only when "
                      "run as root)")
    parser.add_option("-P", "--pidfile", dest="pidfile", metavar="FILE",
                      default="",
                      help="save PID in <file>, only used with -d option")
    # punting on the multipl-v's way of specifying verbosity using optparse
    # I think the way to do this would be to just use getopt instead
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", 
                      default=False,
                      help="verbose (print errors/warnings while in "
                      "event loop)")
    parser.add_option("-w", "--very-verbose", dest="very_verbose", 
                      action="store_true", default=False,
                      help="very verbose (also print client "
                      "commands/responses)")
    parser.add_option("-x", "--extremely-verbose", dest="extremely_verbose", 
                      action="store_true", default=False,
                      help="extremely verbose (also print internal state "
                      "transitions)")

    # WANT TO DO
    # parser.add_option("-U", "--udp-port", dest="udp_port", type="int", 
    #                   default=11211, metavar="PORT",
    #                   help="UDP port number to listen on (default: "
    #                   "%default, 0 is off)")
    # parser.add_option("-s", "--socket", dest="unix_socket", default="", 
    #                   metavar="SOCKET",
    #                   help="UNIX socket path to listen on (disables network "
    #                   "support)")
    # parser.add_option("-a", "--mask", dest="unix_mask", default="0700", 
    #                   metavar="MASK",
    #                   help="access mask for UNIT socket, in octal "
    #                   "(default %default)")
    # parser.add_option("-c", "--connetions", dest="connections", type="int", 
    #                   default=1024, metavar="CONNECTIONS",
    #                   help="max simultaneous connections (default: 1024)")

    # MAYBE LATER
    # parser.add_option("-M", "--memory-error", dest="memory_error", 
    #                   action="store_true", default=False,
    #                   help="return error on memory exhausted (rather than "
    #                   "removing items)")
    # parser.add_option("-i", "--license", dest="license", 
    #                   action="store_true", default=False,
    #                   help="print the jmemcached and libev license")
    # parser.add_option("-D", "--delimiter", dest="delimiter", metavar="CHAR",
    #                   help="Use <char> as the delimiter between key prefixes "
    #                   "and IDs.")
    # parser.add_option("-t", "--threads", dest="threads", type="int", 
    #                   default="4", metavar="THREADS",
    #                   help="number of threads to use (default: %default)")
    # parser.add_option("-R", "--requests", dest="requests", type="int", 
    #                   default=20, metavar="REQUESTS",
    #                   help="Maximum number of requests per event "
    #                   "(default: %default)")
    # parser.add_option("-C", "--disable-cas", dest="disable_cas", 
    #                   action="store_true", default=False,
    #                   help="Disable the use of CAS")
    # parser.add_option("-b", "--backlog", dest="backlog", type="int", 
    #                   default=1024, metavar="BACKLOG",
    #                   help="Set the backlog queue limit (default: %default)")
    # parser.add_option("-B", "--binding", dest="binding", default="auto", 
    #                   metavar="PROTOCOL",
    #                   help="Binding protocol - one of ascii, binary, or "
    #                   "auto (default)")

    # NEVER
    # parser.add_option("-r", "--core-limit", dest="core_limit", 
    #                   action="store_true", default=False,
    #                   help="maximize core file limit")
    # parser.add_option("-k", "--lock-paged", dest="lock_paged", 
    #                   action="store_true", default=False,
    #                   help="lock down all paged memory")
    # parser.add_option("-f", "--factor", dest="factor", type="float", 
    #                   default=1.25, metavar="FACTOR",
    #                   help="chunk size growth factor (default: %default)")
    # parser.add_option("-n", "--minimum-space", dest="minimum_space", 
    #                   type="int", default=48, metavar="SPACE",
    #                   help="minimum space allocated for key+value+flags "
    #                   "(default: %default)")
    # parser.add_option("-L", "--large-memory", dest="large_memory", 
    #                   action="store_true", default=False,
    #                   help="Try to use large memory pages (if available).")
    # parser.add_option("-I", "--item-size", dest="item_size", 
    #                   default="1mb", metavar="SIZE",
    #                   help="Override the size of each slab page. Adjusts "
    #                   "max item size (default: %default, min: 1k, max: 128m)")

    (options, args) = parser.parse_args()
    return options, args

def setup_logging(options):
    """ figure out our logging level and initialize logging """
    if options.extremely_verbose:
        level = mc_log.LOGGING_VVV
    elif options.very_verbose:
        level = mc_log.LOGGING_VV
    elif options.verbose:
        level = mc_log.LOGGING_V
    else:
        level = mc_log.LOGGING_NONE
    mc_log.initialize_logging(level)

def run_as_daemon(options):
    """ run the cache in daemon mode """
    if options.username:
        uid = pwd.getpwnam(options.username).pw_uid
    else:
        uid = os.getuid()
    if options.pidfile:
        pidfile = PIDLockFile(options.pidfile, 5)
        # why not 5?
    else:
        pidfile = None
    with daemon.DaemonContext(uid=uid, pidfile=pidfile):
        server = memcache_connection.Server( 
            interface = options.interface, 
            tcp_port = options.tcp_port, 
            max_bytes = options.max_memory*1024*1024)
        server.start()

def run_it(options):
    """ run the cache in normal mode """
    server = memcache_connection.Server( 
        interface = options.interface, 
        tcp_port = options.tcp_port, 
        max_bytes = options.max_memory*1024*1024)
    server.start()

def main():
    """ run the program """
    options, _ = parse_command_line()
    setup_logging(options)
    if options.daemonize:
        run_as_daemon(options)
    else:
        run_it(options)

if __name__ == "__main__":
    main()
