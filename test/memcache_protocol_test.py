#!/usr/local/bin/python
import memory_cache
import memcache_protocol
import memcache_protocol_execute
import memcache_protocol_parse
import unittest

class TestProtocolBase(unittest.TestCase):

    def setUp(self):
        self.stats = memcache_protocol.ProtocolStats()
        self.mc = memcache_protocol.MCProtocol(self.stats, 
                                               memory_cache.Memcached(self.stats),
                                               ('127.0.0.1', 11211))

    def mc_caller(self, commands):
        for req, resp in commands:
            output = self.mc.got_input(req)
            if resp:
                self.assertTrue(output == resp)

    def mc_except(self, commands, exception):
        with self.assertRaises(exception):
            for req, resp in commands:
                output = self.mc.got_input(req)
                if resp:
                    self.assertTrue(output == resp)

    def test_dump(self):
        stats = self.stats.dump("")
        self.assertTrue(stats is not None)

class TestProtocolStats(unittest.TestCase):

    def setUp(self):
        self.stats = memcache_protocol.ProtocolStats()
        self.mc = memcache_protocol.MCProtocol(self.stats, 
                                               memory_cache.Memcached(self.stats),
                                               ('127.0.0.1', 11211))

    def test_read_bytes(self):
        self.mc.got_input("set test_got_input 0 0 5\r\n12345\r\n")
        self.assertTrue(self.stats.bytes_read == 33)

    def test_write_bytes(self):
        self.mc.got_input("set test_get_output 0 0 5\r\n12345\r\n")
        self.assertTrue(self.stats.bytes_written == 8)

class TestMCProtocol_Parsing(unittest.TestCase):

    def setUp(self):
        self.stats = memcache_protocol.ProtocolStats()
        self.mc = memcache_protocol.MCProtocol(self.stats, 
                                               memory_cache.Memcached(self.stats),
                                               ('127.0.0.1', 11211))

    def test_got_input(self):
        self.mc.got_input("set test_got_input 0 0 5\r\n12345\r\n")
        self.assertTrue(self.mc.memcached.get( ("test_got_input",) )[0][1] == "12345")

    def test_get_output(self):
        output = self.mc.got_input("set test_get_output 0 0 5\r\n12345\r\n")
        self.assertTrue(output == "STORED\r\n")

    def test_multiple_commands(self):
        output = self.mc.got_input("set test_multiple_commands_1 0 0 5\r\n12345\r\n")
        self.assertTrue(output == "STORED\r\n")

        output = self.mc.got_input("set test_multiple_commands_2 0 0 6\r\n123456\r\n")
        self.assertTrue(output == "STORED\r\n")

        output = self.mc.got_input("set test_multiple_commands_3 0 0 7\r\n1234567\r\n")
        self.assertTrue(output == "STORED\r\n")

class TestMCProtocol_Parsing_Partial(unittest.TestCase):

    def setUp(self):
        self.stats = memcache_protocol.ProtocolStats()
        self.mc = memcache_protocol.MCProtocol(self.stats, 
                                               memory_cache.Memcached(self.stats),
                                               ('127.0.0.1', 11211))

    def test_got_input(self):
        self.mc.got_input("set test_got_i")
        self.mc.got_input("nput 0 0 5\r")
        self.mc.got_input("\n12345\r\n")
        self.assertTrue(self.mc.memcached.get( ("test_got_input",) )[0][1] == "12345")

    def test_get_output(self):
        self.mc.got_input("set test_get_output 0 0 5\r\n12")
        self.mc.got_input("345\r")
        output = self.mc.got_input("\n")
        self.assertTrue(output == "STORED\r\n")

class TestMCProtocol_Output(unittest.TestCase):

    def setUp(self):
        self.stats = memcache_protocol.ProtocolStats()
        self.mc = memcache_protocol.MCProtocol(self.stats, 
                                               memory_cache.Memcached(self.stats),
                                               ('127.0.0.1', 11211))

    def test_get_output(self):
        output = self.mc.got_input("set test_cas 0 0 5 noreply\r\n12345\r\n")
        self.assertTrue(output == "")

class TestMCProtocol_Commands(TestProtocolBase):

    def test_set(self):
        self.mc_caller([("set test_set 0 0 5\r\n12345\r\n", "STORED\r\n")])

    def test_cas(self):
        self.mc.got_input("set test_cas 0 0 5\r\n12345\r\n")

        result = self.mc.got_input("gets test_cas\r\n")
        casunique = result.split()[4].split('\r')[0]

        output = self.mc.got_input("cas test_cas 0 0 5 %s\r\n23456\r\n" % casunique)
        self.assertTrue(output == "STORED\r\n")

        output = self.mc.got_input("get test_cas\r\n")
        self.assertTrue(output == "VALUE test_cas 0 5\r\n23456\r\nEND\r\n")

    def test_cas_exists(self):
        self.mc.got_input("set test_cas 0 0 5\r\n12345\r\n")

        result = self.mc.got_input("gets test_cas\r\n")
        casunique = result.split()[4].split('\r')[0]

        self.mc.got_input("set test_cas 0 0 5\r\n67890\r\n")

        output = self.mc.got_input("cas test_cas 0 0 5 %s\r\n23456\r\n" % casunique)
        self.assertTrue(output == "EXISTS\r\n")

        output = self.mc.got_input("get test_cas\r\n")
        self.assertTrue(output == "VALUE test_cas 0 5\r\n67890\r\nEND\r\n")

    def test_cas_not_exists(self):
        output = self.mc.got_input("cas test_cas 0 0 5 555\r\n23456\r\n")
        self.assertTrue(output == "NOT_FOUND\r\n")

    def test_add(self):
        self.mc_caller([("add test_add 0 0 5\r\n12345\r\n", "STORED\r\n")])

    def test_add_exists(self):
        self.mc_caller([("add test_add 0 0 5\r\n12345\r\n", "STORED\r\n"),
                        ("add test_add 0 0 5\r\n67890\r\n", "NOT_STORED\r\n")])

    def test_replace(self):
        self.mc_caller([("set test_replace 0 0 5\r\n12345\r\n", ""),
                        ("replace test_replace 0 0 5\r\n34567\r\n", "STORED\r\n")])

    def test_replace_not_exists(self):
        self.mc_caller([("replace test_replace 0 0 5\r\n34567\r\n", "NOT_STORED\r\n")])

    def test_prepend(self):
        self.mc_caller([("set test_prepend 0 0 5\r\n12345\r\n", ""),
                        ("prepend test_prepend 0 0 1\r\n0\r\n", "STORED\r\n")])

    def test_prepend_not_exist(self):
        self.mc_caller([("prepend test_prepend 0 0 1\r\n0\r\n", "NOT_STORED\r\n")])

    def test_append(self):
        self.mc_caller([("set test_append 0 0 5\r\n12345\r\n", ""),
                        ("append test_append 0 0 1\r\n6\r\n", "STORED\r\n")])

    def test_append_not_exist(self):
        self.mc_caller([("append test_append 0 0 1\r\n6\r\n", "NOT_STORED\r\n")])

    def test_increment(self):
        self.mc_caller([("set test_increment 0 0 5\r\n12345\r\n", ""),
                        ("incr test_increment 1\r\n", "12346\r\n")])

    def test_increment_not_exist(self):
        self.mc_caller([("incr test_increment 1\r\n", "NOT_FOUND\r\n")])

    def test_decrement(self):
        self.mc_caller([("set test_decrement 0 0 5\r\n12345\r\n", ""),
                        ("decr test_decrement 1\r\n", "12344\r\n")])

    def test_decrement_not_exist(self):
        self.mc_caller([("decr test_decrement 1\r\n", "NOT_FOUND\r\n")])

    def test_get(self):
        self.mc_caller([("set test_get 0 0 5\r\n12345\r\n", ""),
                        ("get test_get\r\n", "VALUE test_get 0 5\r\n12345\r\nEND\r\n")])

    def test_gets(self):
        self.mc.got_input("set test_gets 0 0 5\r\n12345\r\n")
        result = self.mc.got_input("gets test_gets\r\n")
        self.assertTrue(result[:20] == "VALUE test_gets 0 5 ")
        self.assertTrue(result[-14:] == "\r\n12345\r\nEND\r\n")

    def test_delete(self):
        self.mc_caller([("set test_delete 0 0 5\r\n12345\r\n", ""),
                        ("delete test_delete 0\r\n", "DELETED\r\n")])

    def test_delete_not_exists(self):
        self.mc_caller([("delete test_delete 0\r\n", "NOT_FOUND\r\n")])

    def test_flush_all_bare(self):
        self.mc_caller([("flush_all\r\n", "OK\r\n")])

    def test_flush_all_no_reply(self):
        output = self.mc.got_input("flush_all noreply\r\n")
        self.assertTrue(output == "")

    def test_flush_all_delay(self):
        self.mc_caller([("flush_all 10\r\n", "OK\r\n")])

    def test_flush_all_delay_no_reply(self):
        output = self.mc.got_input("flush_all 10 noreply\r\n")
        self.assertTrue(output == "")

    def test_version(self):
        self.mc_caller([("version\r\n", "VERSION %s\r\n" % 
                         memcache_protocol_execute.VERSION)])

    def test_stats(self):
        output = self.mc.got_input("stats\r\n")
        self.assertTrue(output is not None)

    def test_stats_settings(self):
        output = self.mc.got_input("stats settings\r\n")
        self.assertTrue(output is not None)

    def test_stats_items(self):
        output = self.mc.got_input("stats items\r\n")
        self.assertTrue(output is not None)

    def test_stats_sizes(self):
        output = self.mc.got_input("stats sizes\r\n")
        self.assertTrue(output is not None)

    def test_stats_slabs(self):
        output = self.mc.got_input("stats slabs\r\n")
        self.assertTrue(output is not None)

    def test_quit(self):
        self.mc_except([("quit\r\n","")], memcache_protocol_execute.QuitException)

class TestMCProtocol_BadCommands(TestProtocolBase):

    def test_bad_command_short(self):
        self.mc_except([("flub\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_command_full(self):
        self.mc_except([("flub test_set 0 0 5\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_delimeter(self):
        self.mc_except([("set test_got_input 0 0 5\r \n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_bytes(self):
        self.mc_except([("set test_set 0 0 5\r\n1234567\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_ending(self):
        self.mc_except([("set test_set 0 0 5\r\n1234567","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_ending2(self):
        self.mc_except([("set test_set 0 0 5\r\n12345get test_set\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_set_args(self):
        self.mc_except([("set test_set 0 0\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_set_flags(self):
        self.mc_except([("set test_set a 0 5\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_set_flags_length(self):
        self.mc_except([("set test_set aaa 0 5\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_set_exptime(self):
        self.mc_except([("set test_set 0 a 5\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_set_bytes(self):
        self.mc_except([("set test_set 0 0 a\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_cas_args(self):
        self.mc_except([("cas test_cas 0 0 5\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_cas_flags(self):
        self.mc_except([("cas test_cas a 0 0 5\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_cas_flags_length(self):
        self.mc_except([("cas test_cas 00 0 0 5\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_cas_exptime(self):
        self.mc_except([("cas test_cas 0 a 5 100\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_cas_bytes(self):
        self.mc_except([("cas test_cas 0 0 a 100\r\n12345\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_get_args(self):
        self.mc_except([("get\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_delete_args(self):
        self.mc_except([("delete\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_incr_args(self):
        self.mc_except([("incr test_incr\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_incr_value(self):
        self.mc_caller([("set test_incr 0 0 5\r\naaaaa\r\n", "STORED\r\n"),
                        ("incr test_incr 1\r\n",
                         "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")])

    def test_bad_incr_not_number(self):
        self.mc_except([("set test_incr 0 0 5\r\n12345\r\n", "STORED\r\n"),
                        ("incr test_incr a\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_decr_args(self):
        self.mc_except([("decr test_decr\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_decr_value(self):
        self.mc_caller([("set test_decr 0 0 5\r\naaaaa\r\n", "STORED\r\n"),
                        ("decr test_decr 1\r\n",
                         "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")])

    def test_bad_decr_not_number(self):
        self.mc_except([("set test_decr 0 0 5\r\n12345\r\n", "STORED\r\n"),
                        ("decr test_decr a\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_flush_all_delay(self):
        self.mc_except([("flush_all a\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

    def test_bad_stats(self):
        self.mc_except([("stats flub\r\n","")], 
                       memcache_protocol_parse.ProtocolException)

class TestExecute(unittest.TestCase):
    def test_bad_command(self):
        cmd = memcache_protocol_parse.MCCommand(command='flub')
        with self.assertRaises(memcache_protocol_execute.ExecuteException):
            memcache_protocol_execute.execute_command(cmd, None, '')

if __name__ == "__main__":
    unittest.main()
