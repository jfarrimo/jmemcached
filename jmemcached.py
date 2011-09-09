import memcache_connection
import optparse

def parse_command_line():
    parser = optparse.OptionParser()

    # HAVE
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
    parser.add_option("-U", "--udp-port", dest="udp_port", type="int", 
                      default=11211, metavar="PORT",
                      help="UDP port number to listen on (default: "
                      "%default, 0 is off)")
    parser.add_option("-s", "--socket", dest="unix_socket", default="", 
                      metavar="SOCKET",
                      help="UNIX socket path to listen on (disables network "
                      "support)")
    parser.add_option("-a", "--mask", dest="unix_mask", default="0700", 
                      metavar="MASK",
                      help="access mask for UNIT socket, in octal "
                      "(default %default)")

    # DOING
    # parser.add_option("-v", "--verbose", dest="verbose", action="store_true", 
    #                   default=False,
    #                   help="verbose (print errors/warnings while in "
    #                   "event loop)")
    # parser.add_option("-vv", "--very-verbose", dest="very_verbose", 
    #                   action="store_true", default=False,
    #                   help="very verbose (also print client "
    #                   "commands/responses)")
    # parser.add_option("-vvv", "--extremely-verbose", dest="extremely_verbose", 
    #                   action="store_true", default=False,
    #                   help="extremely verbose (also print internal state "
    #                   "transitions)")

    # WANT TO DO
    # parser.add_option("-d", "--daemonize", dest="daemonize", 
    #                   action="store_true", default=False,
    #                   help="run as a daemon")
    # parser.add_option("-u", "--username", dest="username", metavar="USERNAME",
    #                   help="assume identity of <username> (only when "
    #                   "run as root)")
    # parser.add_option("-P", "--pidfile", dest="pidfile", metavar="FILE",
    #                   help="save PID in <file>, only used with -d option")
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

def main():
    options, _ = parse_command_line()
    server = memcache_connection.Server( 
        interface = options.interface, 
        tcp_port = options.tcp_port, 
        udp_port = options.udp_port, 
        unix_socket = options.unix_socket,
        unix_mask = options.unix_mask,
        max_bytes = options.max_memory*1024*1024)
    server.start()

if __name__ == "__main__":
    main()
