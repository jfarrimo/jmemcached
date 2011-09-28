"""
All the basic things to implement memory cache.

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
import time
import zlib

def unique_hash(item):
    """
    unique hash for an object
    
    not sure how unique this really is...
    """
    return zlib.crc32(str(id(item))) & 0xffffffff

def int_time():
    """ time seconds as an integer """
    return int(time.time())

class CacheItem(object):
    """
    a single item in the cache
    """
    TIME_CUTOFF = 60*60*24*30 # this means things get weird in Jan. 1970

    def __init__(self, key, value, flags, exptime):
        self.key = key
        self.value = value
        self.flags = flags
        self.exptime = self.prep_exptime(exptime)

        self.prev = None
        self.next = None

    def prep_exptime(self, exptime):
        """
        make exptime relative to current time
        """
        exptime = int(exptime)
        if exptime <= self.TIME_CUTOFF and exptime > 0:
            return int_time() + exptime
        else:
            return exptime

    def set_exptime(self, exptime):
        """ set exptime """
        self.exptime = self.prep_exptime(exptime)

    def casunique(self):
        """ get the casunique value """
        return unique_hash(self)

    def has_expired(self):
        """ has this item gone past its expire time? """
        return self.exptime > 0 and self.exptime <= int_time()

    def bytes(self):
        """ byte count """
        return len(self.key) + len(self.value) + len(self.flags)

class LRU(object):
    """
    least recently used list
    """
    def __init__(self):
        self.head = None
        self.tail = None

    def add(self, item):
        """ add an item to the head of the list """
        item.next = self.head
        item.prev = None

        if self.head is not None:
            self.head.prev = item
        self.head = item

        if self.tail is None:
            self.tail = item

    def remove(self, item):
        """ remove an item from the tail of the list """
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
        """ clear the list """
        self.remove(item)
        self.add(item)

    def least(self):
        """ get the oldest item (tail of list) """
        return self.tail

class MemoryCacheStats(object):
    """ statistics for cache primitives """
    def __init__(self):
        self.limit_maxbytes = 0
        self.limit_maxitems = 0
        self.curr_items = 0
        self.total_items = 0
        self.bytes = 0
        self.evictions = 0
        self.reclaimed = 0

    def set_maximums(self, max_items, max_bytes):
        """ maximums were set """
        self.limit_maxitems = max_items
        self.limit_maxbytes = max_bytes

    def add_item(self, add_bytes):
        """ item added """
        self.curr_items += 1
        self.total_items += 1
        self.bytes += add_bytes

    def del_item(self, del_bytes):
        """ item deleted """
        self.curr_items -= 1
        self.bytes -= del_bytes

    def evict(self):
        """ item evicted """
        self.evictions += 1

    def expire(self):
        """ item expired """
        self.reclaimed += 1

    def dump(self, _):
        """ dump the statistics """
        ret = [('limit_maxbytes', self.limit_maxbytes),
               ('limit_maxitems', self.limit_maxitems),
               ('curr_items', self.curr_items),
               ('total_items', self.total_items),
               ('bytes', self.bytes),
               ('evictions', self.evictions),
               ('reclaimed', self.reclaimed)]
        return ret

class MemoryCache(object):
    """
    the basic elements needed to create memcached commands
    """
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
        """ evict if too many items or too many bytes """
        while self.byte_count + added_bytes > self.max_bytes:
            self.delete(self.lru.least())
            self.stats.evict()

        while self.item_count + added_items > self.max_items:
            self.delete(self.lru.least())
            self.stats.evict()

    def _remove(self, item):
        """ remove an item from the cache """
        byte_count = item.bytes()
        self.byte_count -= byte_count
        self.item_count -= 1
        self.lru.remove(item)
        self.stats.del_item(byte_count)

    def get(self, key):
        """ get an item from the cache """
        if key in self.the_cache:
            if self.the_cache[key].has_expired():
                self.delete(self.the_cache[key])
                self.stats.expire()
            else:
                return self.the_cache[key]
        return None

    def add(self, key, value, flags, exptime):
        """ add an item to the cache """
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
        """ replace an item in the cache """
        key = old_item.key
        if flags is None:
            flags = old_item.flags
        if exptime is None:
            exptime = old_item.exptime

        self._remove(old_item)
        return self.add(key, value, flags, exptime)

    def delete(self, item):
        """ delete an item from the cache """
        self._remove(item)
        del self.the_cache[item.key]

    def flush(self, delay):
        """ expire all the items in the cache """
        exp_time = int_time() + int(delay)
        for key in self.the_cache:
            self.the_cache[key].set_exptime(exp_time)

    def touch(self, item):
        """ note item access """
        self.lru.reset(item)
