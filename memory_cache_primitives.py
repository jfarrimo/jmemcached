import logging
import time
import zlib

logging.basicConfig(level=logging.DEBUG)

def unique_hash(item):
    return zlib.crc32(str(id(item))) & 0xffffffff

def int_time():
    return int(time.time())

class CacheItem(object):
    TIME_CUTOFF = 60*60*24*30 # this means things get weird in Jan. 1970

    def __init__(self, key, value, flags, exptime):
        self.key = key
        self.value = value
        self.flags = flags
        self.exptime = self.prep_exptime(exptime)

        self.prev = None
        self.next = None

    def prep_exptime(self, exptime):
        exptime = int(exptime)
        if exptime <= self.TIME_CUTOFF and exptime > 0:
            return int_time() + exptime
        else:
            return exptime

    def set_exptime(self, exptime):
        self.exptime = self.prep_exptime(exptime)

    def casunique(self):
        return unique_hash(self)

    def has_expired(self):
        return self.exptime > 0 and self.exptime <= int_time()

    def bytes(self):
        return len(self.key) + len(self.value) + len(self.flags)

class LRU(object):
    def __init__(self):
        self.head = None
        self.tail = None

    def add(self, item):
        item.next = self.head
        item.prev = None

        if self.head is not None:
            self.head.prev = item
        self.head = item

        if self.tail is None:
            self.tail = item

    def remove(self, item):
        if self.head is item:
            self.head = item.next
        if self.tail is item:
            self.tail = item.prev

        if item.prev is not None:
            item.prev.next = item.next
        if item.next is not None:
            item.next.prev = item.prev

        # make sure no more references, so everything can be
        # garbage collected when necessary
        item.prev = None
        item.next = None

    def reset(self, item):
        self.remove(item)
        self.add(item)

    def least(self):
        return self.tail

class MemoryCacheStats(object):
    def __init__(self):
        self.limit_maxbytes = 0
        self.limit_maxitems = 0
        self.curr_items = 0
        self.total_items = 0
        self.bytes = 0
        self.evictions = 0
        self.reclaimed = 0

    def set_maximums(self, max_items, max_bytes):
        self.limit_maxitems = max_items
        self.limit_maxbytes = max_bytes

    def add_item(self, add_bytes):
        self.curr_items += 1
        self.total_items += 1
        self.bytes += add_bytes

    def del_item(self, del_bytes):
        self.curr_items -= 1
        self.bytes -= del_bytes

    def evict(self):
        self.evictions += 1

    def expire(self):
        self.reclaimed += 1

    def dump(self, command): # pylint: disable=W0613
        ret = [('limit_maxbytes', self.limit_maxbytes),
               ('limit_maxitems', self.limit_maxitems),
               ('curr_items', self.curr_items),
               ('total_items', self.total_items),
               ('bytes', self.bytes),
               ('evictions', self.evictions),
               ('reclaimed', self.reclaimed)]
        return ret

class MemoryCache(object):
    def __init__(self, stats, max_items, max_bytes):
        self.stats = stats
        self.stats.set_maximums(max_items, max_bytes)

        self.the_cache = {}
        self.lru = LRU()

        self.byte_count = 0
        self.max_bytes = max_bytes

        self.item_count = 0
        self.max_items = max_items

    def _evict(self, added_bytes=0, added_items=1):
        while self.byte_count + added_bytes > self.max_bytes:
            self.delete(self.lru.least())
            self.stats.evict()

        while self.item_count + added_items > self.max_items:
            self.delete(self.lru.least())
            self.stats.evict()

    def _remove(self, item):
        byte_count = item.bytes()
        self.byte_count -= byte_count
        self.item_count -= 1
        self.lru.remove(item)
        self.stats.del_item(byte_count)

    def get(self, key):
        if key in self.the_cache:
            if self.the_cache[key].has_expired():
                self.delete(self.the_cache[key])
                self.stats.expire()
            else:
                return self.the_cache[key]
        return None

    def add(self, key, value, flags, exptime):
        self._evict(len(value))

        new_item = CacheItem(key, value, flags, exptime)
        new_bytes = new_item.bytes()
        self.the_cache[key] = new_item
        self.byte_count += new_bytes
        self.item_count += 1
        self.lru.add(new_item)
        self.stats.add_item(new_bytes)

        return new_item
    
    def replace(self, old_item, value, flags=None, exptime=None):
        key = old_item.key
        if flags is None:
            flags = old_item.flags
        if exptime is None:
            exptime = old_item.exptime

        self._remove(old_item)
        return self.add(key, value, flags, exptime)

    def delete(self, item):
        self._remove(item)
        del self.the_cache[item.key]

    def flush(self, delay):
        exp_time = int_time() + int(delay)
        for key in self.the_cache:
            self.the_cache[key].set_exptime(exp_time)

    def touch(self, item):
        self.lru.reset(item)
