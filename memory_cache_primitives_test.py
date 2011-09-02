#!/usr/local/bin/python
import memory_cache_primitives
import time
import unittest

class TestCacheItem(unittest.TestCase):

    def test_exptime_never(self):
        item = memory_cache_primitives.CacheItem('key', 'value', '0', '0')
        self.assertTrue(item.exptime == 0)

    def test_exptime_sooner(self):
        item = memory_cache_primitives.CacheItem('key', 'value', '0', '10')
        self.assertTrue(item.exptime > 10)

    def test_exptime_later(self):
        exptime = memory_cache_primitives.CacheItem.TIME_CUTOFF + 10
        item = memory_cache_primitives.CacheItem('key', 'value', '0', str(exptime))
        self.assertTrue(item.exptime == exptime)

    def test_casunique_same(self):
        item = memory_cache_primitives.CacheItem('key', 'value', '0', '0')
        self.assertTrue(item.casunique() == item.casunique())

    def test_casunique_different(self):
        item1 = memory_cache_primitives.CacheItem('key', 'value', '0', '0')
        item2 = memory_cache_primitives.CacheItem('key', 'value', '0', '0')
        self.assertTrue(item1.casunique() != item2.casunique())

class TestLRU(unittest.TestCase):

    def setUp(self):
        self.lru = memory_cache_primitives.LRU()

        self.item0 = memory_cache_primitives.CacheItem('key', 'value', '0', '0')
        self.item1 = memory_cache_primitives.CacheItem('key', 'value', '0', '0')
        self.item2 = memory_cache_primitives.CacheItem('key', 'value', '0', '0')
        self.item3 = memory_cache_primitives.CacheItem('key', 'value', '0', '0')
        self.item4 = memory_cache_primitives.CacheItem('key', 'value', '0', '0')

    def test_add1(self):
        self.lru.add(self.item0)
        self.assertTrue(self.lru.least() == self.item0)

    def test_add5(self):
        self.lru.add(self.item0)
        self.lru.add(self.item1)
        self.lru.add(self.item2)
        self.lru.add(self.item3)
        self.lru.add(self.item4)
        self.assertTrue(self.lru.least() == self.item0)

class TestLRU_Remove(TestLRU):

    def setUp(self):
        super(TestLRU_Remove, self).setUp()

        self.lru.add(self.item0)
        self.lru.add(self.item1)
        self.lru.add(self.item2)
        self.lru.add(self.item3)
        self.lru.add(self.item4)

    def test_remove_1_back(self):
        self.lru.remove(self.item0)
        self.assertTrue(self.lru.least() == self.item1)

    def test_remove_1_front(self):
        self.lru.remove(self.item4)
        self.assertTrue(self.lru.least() == self.item0)

    def test_remove_1_middle(self):
        self.lru.remove(self.item2)
        self.assertTrue(self.lru.least() == self.item0)

    def test_remove_3_back(self):
        self.lru.remove(self.item0)
        self.lru.remove(self.item1)
        self.lru.remove(self.item2)
        self.assertTrue(self.lru.least() == self.item3)

    def test_remove_3_front(self):
        self.lru.remove(self.item4)
        self.lru.remove(self.item3)
        self.lru.remove(self.item2)
        self.assertTrue(self.lru.least() == self.item0)

    def test_remove_3_middle(self):
        self.lru.remove(self.item1)
        self.lru.remove(self.item2)
        self.lru.remove(self.item3)
        self.assertTrue(self.lru.least() == self.item0)

    def test_remove_all_back(self):
        self.lru.remove(self.item0)
        self.lru.remove(self.item1)
        self.lru.remove(self.item2)
        self.lru.remove(self.item3)
        self.lru.remove(self.item4)
        self.assertTrue(self.lru.least() is None)

    def test_remove_all_front(self):
        self.lru.remove(self.item4)
        self.lru.remove(self.item3)
        self.lru.remove(self.item2)
        self.lru.remove(self.item1)
        self.lru.remove(self.item0)
        self.assertTrue(self.lru.least() is None)

    def test_reset(self):
        self.assertTrue(self.lru.least() == self.item0)
        self.lru.reset(self.item0)
        self.assertTrue(self.lru.least() == self.item1)

class TestMemoryCacheStats(unittest.TestCase):

    def setUp(self):
        self.stats = memory_cache_primitives.MemoryCacheStats()
        self.mc = memory_cache_primitives.MemoryCache(self.stats, 1000, 100000)

    def test_maximums(self):
        self.assertTrue(self.stats.limit_maxitems == 1000)
        self.assertTrue(self.stats.limit_maxbytes == 100000)

    def test_add(self):
        self.mc.add('key1', 'value1', '0', '0')
        self.mc.add('key2', 'value2', '0', '0')
        self.mc.add('key3', 'value3', '0', '0')
        self.mc.add('key4', 'value4', '0', '0')
        self.mc.add('key5', 'value5', '0', '0')

        self.assertTrue(self.stats.curr_items == 5)
        self.assertTrue(self.stats.total_items == 5)
        self.assertTrue(self.stats.bytes == 55)

    def test_evict_count(self):
        self.stats = memory_cache_primitives.MemoryCacheStats()
        self.mc = memory_cache_primitives.MemoryCache(self.stats, 2, 100000)

        self.mc.add('key1', 'value1', '0', '0')
        self.mc.add('key2', 'value2', '0', '0')
        self.mc.add('key3', 'value3', '0', '0')
        self.mc.add('key4', 'value4', '0', '0')
        self.mc.add('key5', 'value5', '0', '0')

        self.assertTrue(self.stats.curr_items == 2)
        self.assertTrue(self.stats.total_items == 5)
        self.assertTrue(self.stats.bytes == 22)

    def test_evict_size(self):
        self.stats = memory_cache_primitives.MemoryCacheStats()
        self.mc = memory_cache_primitives.MemoryCache(self.stats, 1000, 20)

        self.mc.add('key1', '12345', '0', '0')
        self.mc.add('key2', '67890', '0', '0')
        self.mc.add('key3', 'abcde', '0', '0')
        self.mc.add('key4', 'fghij', '0', '0')
        self.mc.add('key5', 'klmno', '0', '0')

        self.assertTrue(self.stats.curr_items == 2)
        self.assertTrue(self.stats.total_items == 5)
        self.assertTrue(self.stats.bytes == 20)

    def test_replace(self):
        self.mc.add('key1', 'value1', '0', '0')
        self.mc.add('key2', 'value2', '0', '0')
        self.mc.add('key3', 'value3', '0', '0')
        self.mc.add('key4', 'value4', '0', '0')
        self.mc.add('key5', 'value5', '0', '0')

        self.assertTrue(self.stats.curr_items == 5)
        self.assertTrue(self.stats.total_items == 5)
        self.assertTrue(self.stats.bytes == 55)

        item = self.mc.get('key3')
        self.mc.replace(item, 'value33', '0', '0')

        self.assertTrue(self.stats.curr_items == 5)
        self.assertTrue(self.stats.total_items == 6)
        self.assertTrue(self.stats.bytes == 56)

    def test_delete(self):
        self.mc.add('key1', 'value1', '0', '0')
        self.mc.add('key2', 'value2', '0', '0')
        self.mc.add('key3', 'value3', '0', '0')
        self.mc.add('key4', 'value4', '0', '0')
        self.mc.add('key5', 'value5', '0', '0')

        self.assertTrue(self.stats.curr_items == 5)
        self.assertTrue(self.stats.total_items == 5)
        self.assertTrue(self.stats.bytes == 55)

        item = self.mc.get('key3')
        self.mc.delete(item)

        self.assertTrue(self.stats.curr_items == 4)
        self.assertTrue(self.stats.total_items == 5)
        self.assertTrue(self.stats.bytes == 44)

    def test_dump(self):
        stats = self.stats.dump("")
        self.assertTrue(stats is not None)

class TestMemoryCache(unittest.TestCase):

    def setUp(self):
        self.stats = memory_cache_primitives.MemoryCacheStats()
        self.mc = memory_cache_primitives.MemoryCache(self.stats, 1000, 100000)

    def test_evict_count(self):
        self.stats = memory_cache_primitives.MemoryCacheStats()
        self.mc = memory_cache_primitives.MemoryCache(self.stats, 2, 100000)

        self.mc.add('key1', 'value1', '0', '0')
        self.mc.add('key2', 'value2', '0', '0')
        self.mc.add('key3', 'value3', '0', '0')
        self.mc.add('key4', 'value4', '0', '0')
        self.mc.add('key5', 'value5', '0', '0')

        item = self.mc.get('key1')
        self.assertTrue(item is None)

        item = self.mc.get('key5')
        self.assertTrue(item.value == 'value5')

    def test_evict_size(self):
        self.stats = memory_cache_primitives.MemoryCacheStats()
        self.mc = memory_cache_primitives.MemoryCache(self.stats, 1000, 10)

        self.mc.add('key1', '12345', '0', '0')
        self.mc.add('key2', '67890', '0', '0')
        self.mc.add('key3', 'abcde', '0', '0')
        self.mc.add('key4', 'fghij', '0', '0')
        self.mc.add('key5', 'klmno', '0', '0')

        item = self.mc.get('key1')
        self.assertTrue(item is None)

        item = self.mc.get('key5')
        self.assertTrue(item.value == 'klmno')

    def test_add(self):
        self.mc.add('key', 'value', '0', '0')
        item = self.mc.get('key')
        self.assertTrue(item.value == 'value')

    def test_replace(self):
        self.mc.add('key', 'value', '0', '0')
        item = self.mc.get('key')
        self.assertTrue(item.value == 'value')

        self.mc.replace(item, 'value2', '0', '0')
        item = self.mc.get('key')
        self.assertTrue(item.value == 'value2')

    def test_replace_old_values(self):
        self.mc.add('key', 'value', '0', '0')
        item = self.mc.get('key')
        self.assertTrue(item.value == 'value')

        self.mc.replace(item, 'value2')
        item = self.mc.get('key')
        self.assertTrue(item.value == 'value2')

    def test_delete(self):
        self.mc.add('key', 'value', '0', '0')
        item = self.mc.get('key')
        self.assertTrue(item.value == 'value')

        self.mc.delete(item)
        item = self.mc.get('key')
        self.assertTrue(item is None)

    def test_touch(self):
        self.mc.add('key', 'value', '0', '0')
        item = self.mc.get('key')
        self.assertTrue(item.value == 'value')

        self.mc.touch(item)
        item = self.mc.get('key')
        self.assertTrue(item.value == 'value')

    def test_flush(self):
        self.mc.add('key1', '12345', '0', '0')
        self.mc.add('key2', '67890', '0', '0')
        self.mc.add('key3', 'abcde', '0', '0')
        self.mc.add('key4', 'fghij', '0', '0')
        self.mc.add('key5', 'klmno', '0', '0')
        self.mc.flush(0)
        item = self.mc.get('key1')
        self.assertTrue(item is None)

    def test_get(self):
        self.mc.add('key', 'value', '0', '0')
        item = self.mc.get('key')
        self.assertTrue(item.value == 'value')

    def test_get_expired(self):
        exp_time = int(time.time()) - 10

        self.mc.add('key', 'value', '0', str(exp_time))
        item = self.mc.get('key')
        self.assertTrue(item is None)

if __name__ == "__main__":
    unittest.main()
