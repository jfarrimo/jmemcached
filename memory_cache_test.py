#!/usr/local/bin/python
import memory_cache
import time
import unittest

class TestMemcachedStats(unittest.TestCase):

    def setUp(self):
        self.stats = memory_cache.MemcachedStats()
        self.mc = memory_cache.Memcached(self.stats)

    def test_set(self):
        self.mc.set("test_set", "0", "0", "12345")
        self.assertTrue(self.stats.cmd_set == 1)

    def test_get(self):
        self.mc.set("test_get", "0", "0", "12345")
        self.mc.get( ("test_get",) )
        self.assertTrue(self.stats.cmd_get == 1)
        self.assertTrue(self.stats.get_hits == 1)

    def test_get_miss(self):
        self.mc.get( ("test_get",) )
        self.assertTrue(self.stats.cmd_get == 1)
        self.assertTrue(self.stats.get_misses == 1)

    def test_gets(self):
        self.mc.set("test_gets", "0", "0", "12345")
        self.mc.gets( ("test_gets",) )
        self.assertTrue(self.stats.cmd_get == 1)
        self.assertTrue(self.stats.get_hits == 1)

    def test_delete(self):
        self.mc.set("test_delete", "0", "0", "12345")
        self.mc.delete("test_delete")
        self.assertTrue(self.stats.delete_hits == 1)

    def test_delete_not_exist(self):
        self.mc.delete("test_delete")
        self.assertTrue(self.stats.delete_misses == 1)

    def test_increment(self):
        self.mc.set("test_increment", "0", "0", "12345")
        self.mc.increment("test_increment", "1")
        self.assertTrue(self.stats.incr_hits == 1)

    def test_increment_not_exist(self):
        self.mc.increment("test_increment", "1")
        self.assertTrue(self.stats.incr_misses == 1)

    def test_decrement(self):
        self.mc.set("test_decrement", "0", "0", "12345")
        self.mc.decrement("test_decrement", "1")
        self.assertTrue(self.stats.decr_hits == 1)

    def test_decrement_not_exist(self):
        self.mc.decrement("test_decrement", "1")
        self.assertTrue(self.stats.decr_misses == 1)

    def test_cas_not_exist(self):
        self.mc.cas("test_cas", "0", "0", "1", "54321")
        self.assertTrue(self.stats.cas_misses == 1)

    def test_cas_exist(self):
        self.mc.set("test_cas", "0", "0", "12345")

        tester = self.mc.gets( ("test_cas",) )[0]
        self.assertTrue(tester[1] == "12345")
        cas = tester[3]

        self.mc.cas("test_cas", "0", "0", cas, "54321")

        self.assertTrue(self.stats.cas_hits == 1)

    def test_cas_changed(self):
        self.mc.set("test_cas", "0", "0", "12345")

        tester = self.mc.gets( ("test_cas",) )[0]
        self.assertTrue(tester[1] == "12345")
        cas1 = tester[3]

        self.mc.set("test_cas", "0", "0", "12345")

        tester = self.mc.gets( ("test_cas",) )[0]
        self.assertTrue(tester[1] == "12345")
        cas2 = tester[3]

        self.mc.cas("test_cas", "0", "0", cas1, "54321")

        self.assertTrue(self.stats.cas_badvals == 1)

class TestMemcached(unittest.TestCase):

    def setUp(self):
        self.stats = memory_cache.MemcachedStats()
        self.mc = memory_cache.Memcached(self.stats)

    def test_set(self):
        self.mc.set("test_set", "0", "0", "12345")
        self.assertTrue(self.mc.get( ("test_set",) )[0][1] == "12345")

    def test_set_exist(self):
        self.mc.set("test_set", "0", "0", "00000")
        self.mc.set("test_set", "0", "0", "12345")
        self.assertTrue(self.mc.get( ("test_set",) )[0][1] == "12345")

    def test_cas_constant_unique(self):
        self.mc.set("test_cas", "0", "0", "12345")
        tester = self.mc.gets( ("test_cas",) )[0]
        self.assertTrue(tester[1] == "12345")
        cas = tester[3]
        tester = self.mc.gets( ("test_cas",) )[0]
        self.assertTrue(cas == tester[3])

    def test_cas_not_exist(self):
        self.mc.cas("test_cas", "0", "0", "1", "54321")
        self.assertTrue(self.mc.get( ("test_cas",) )[0][1] == "54321")

    def test_cas_exist(self):
        self.mc.set("test_cas", "0", "0", "12345")

        tester = self.mc.gets( ("test_cas",) )[0]
        self.assertTrue(tester[1] == "12345")
        cas = tester[3]

        self.mc.cas("test_cas", "0", "0", cas, "54321")

        self.assertTrue(self.mc.get( ("test_cas",) )[0][1] == "54321")

    def test_cas_changed(self):
        self.mc.set("test_cas", "0", "0", "12345")

        tester = self.mc.gets( ("test_cas",) )[0]
        self.assertTrue(tester[1] == "12345")
        cas1 = tester[3]

        self.mc.set("test_cas", "0", "0", "12345")

        tester = self.mc.gets( ("test_cas",) )[0]
        self.assertTrue(tester[1] == "12345")
        cas2 = tester[3]

        self.mc.cas("test_cas", "0", "0", cas1, "54321")

        self.assertTrue(self.mc.get( ("test_cas",) )[0][1] == "12345")

    def test_add(self):
        self.mc.set("test_add", "0", "0", "12345")
        self.mc.add("test_add", "0", "0", "34567")
        self.assertTrue(self.mc.get( ("test_add",) )[0][1] == "12345")

    def test_add_not_exist(self):
        self.mc.add("test_add", "0", "0", "12345")
        self.assertTrue(self.mc.get( ("test_add",) )[0][1] == "12345")

    def test_replace(self):
        self.mc.set("test_replace", "0", "0", "12345")
        self.mc.replace("test_replace", "0", "0", "3456")
        self.assertTrue(self.mc.get( ("test_replace",) )[0][1] == "3456")

    def test_replace_not_exist(self):
        self.assertTrue(self.mc.replace("test_replace", "0", "0", "3456") == self.mc.NOT_STORED)

    def test_prepend(self):
        self.mc.set("test_prepend", "0", "0", "12345")
        self.mc.prepend("test_prepend", "0", "0", "0")
        self.assertTrue(self.mc.get( ("test_prepend",) )[0][1] == "012345")

    def test_prepend_not_exist(self):
        self.assertTrue(self.mc.prepend("test_prepend", "0", "0", "0") == self.mc.NOT_STORED)

    def test_append(self):
        self.mc.set("test_append", "0", "0", "12345")
        self.mc.append("test_append", "0", "0", "6")
        self.assertTrue(self.mc.get( ("test_append",) )[0][1] == "123456")

    def test_append_not_exist(self):
        self.assertTrue(self.mc.append("test_append", "0", "0", "6") == self.mc.NOT_STORED)

    def test_increment(self):
        self.mc.set("test_increment", "0", "0", "12345")
        self.mc.increment("test_increment", "1")
        self.assertTrue(self.mc.get( ("test_increment",) )[0][1] == "12346")

    def test_increment_not_exist(self):
        self.assertTrue(self.mc.increment("test_increment", "1")[0] == self.mc.NOT_FOUND)

    def test_increment_not_number(self):
        self.mc.set("test_increment", "0", "0", "aaaaa")
        self.assertTrue(self.mc.increment("test_increment", "1")[0] == self.mc.NOT_NUMBER)

    def test_decrement(self):
        self.mc.set("test_decrement", "0", "0", "12345")
        self.mc.decrement("test_decrement", "1")
        self.assertTrue(self.mc.get( ("test_decrement",) )[0][1] == "12344")

    def test_decrement_not_exist(self):
        self.assertTrue(self.mc.decrement("test_decrement", "1")[0] == self.mc.NOT_FOUND)

    def test_decrement_not_number(self):
        self.mc.set("test_decrement", "0", "0", "aaaaa")
        self.assertTrue(self.mc.decrement("test_decrement", "1")[0] == self.mc.NOT_NUMBER)

    def test_get(self):
        self.mc.set("test_get", "0", "0", "12345")
        self.assertTrue(self.mc.get( ("test_get",) )[0][1] == "12345")

    def test_gets(self):
        self.mc.set("test_gets", "0", "0", "12345")
        self.assertTrue(self.mc.gets( ("test_gets",) )[0][1] == "12345")

    def test_delete(self):
        self.mc.set("test_delete", "0", "0", "12345")
        self.assertTrue(self.mc.get( ("test_delete",) )[0][1] == "12345")
        self.mc.delete("test_delete")
        self.assertTrue(not self.mc.get( ("test_delete",) ))

    def test_delete_not_exist(self):
        self.mc.delete("test_delete")
        self.assertTrue(not self.mc.get( ("test_delete",) ))

    def test_flush(self):
        self.mc.flush(0)

    def test_stats(self):
        stats = self.mc.stats("")
        self.assertTrue(stats is not None)

if __name__ == "__main__":
    unittest.main()
