import logging
import sys

import memory_cache_primitives

logging.basicConfig(level=logging.DEBUG)

class MemcachedStats(memory_cache_primitives.MemoryCacheStats):
    def __init__(self):
        super(MemcachedStats, self).__init__()

        self.cmd_get = 0
        self.cmd_set = 0
        self.get_misses = 0
        self.get_hits = 0
        self.delete_misses = 0
        self.delete_hits = 0
        self.incr_misses = 0
        self.incr_hits = 0
        self.decr_misses = 0
        self.decr_hits = 0
        self.cas_misses = 0
        self.cas_hits = 0
        self.cas_badvals = 0
        self.auth_cmds = 0
        self.auth_errors = 0

    def set(self):
        self.cmd_set += 1

    def get(self, hit):
        self.cmd_get += 1
        if hit:
            self.get_hits += 1
        else:
            self.get_misses += 1

    def delete(self, hit):
        if hit:
            self.delete_hits += 1
        else:
            self.delete_misses += 1

    def incr(self, hit):
        if hit:
            self.incr_hits += 1
        else:
            self.incr_misses += 1

    def decr(self, hit):
        if hit:
            self.decr_hits += 1
        else:
            self.decr_misses += 1

    def cas_miss(self):
        self.cas_misses += 1

    def cas_hit(self):
        self.cas_hits += 1

    def cas_badval(self):
        self.cas_badvals += 1

    def dump(self, command):
        ret_super = super(MemcachedStats, self).dump(command)
        ret = [('cmd_get', self.cmd_get),
               ('cmd_set', self.cmd_set),
               ('get_misses', self.get_misses),
               ('get_hits', self.get_hits),
               ('delete_misses', self.delete_misses),
               ('delete_hits', self.delete_hits),
               ('incr_misses', self.incr_misses),
               ('incr_hits', self.incr_hits),
               ('decr_misses', self.decr_misses),
               ('decr_hits', self.decr_hits),
               ('cas_misses', self.cas_misses),
               ('cas_badvals', self.cas_badvals),
               ('auth_cmds', self.auth_cmds),
               ('auth_errors', self.auth_errors)]
        ret_super.extend(ret)
        return ret_super

DEFAULT_MAX_BYTES = sys.maxint
DEFAULT_MAX_ITEMS = sys.maxint

class Memcached(object):
    DELETED = 0
    EXISTS = 1
    NOT_FOUND = 2
    NOT_NUMBER = 3
    NOT_STORED = 4
    STORED = 5

    def __init__(self, stats, max_items=DEFAULT_MAX_ITEMS, 
                 max_bytes=DEFAULT_MAX_BYTES):
        self._stats = stats
        self.mc = memory_cache_primitives.MemoryCache(
            self._stats, max_items, max_bytes)

    def set(self, key, flags, exptime, value):
        item = self.mc.get(key)
        if item is not None:
            self.mc.replace(item, value, flags, exptime)
        else:
            self.mc.add(key, value, flags, exptime)
        self._stats.set()
        return self.STORED

    def cas(self, key, flags, exptime, casunique, value):
        item = self.mc.get(key)
        if item is None:
            self.mc.add(key, value, flags, exptime)
            self._stats.cas_miss()
            return self.NOT_FOUND
        elif item.casunique() == int(casunique):
            self.mc.replace(item, value, flags, exptime)
            self._stats.cas_hit()
            return self.STORED
        else:
            self._stats.cas_badval()
            return self.EXISTS

    def add(self, key, flags, exptime, value):
        item = self.mc.get(key)
        if item is not None:
            self.mc.touch(item)
            return self.NOT_STORED
        else:
            self.mc.add(key, value, flags, exptime)
            return self.STORED

    def replace(self, key, flags, exptime, value):
        item = self.mc.get(key)
        if item is not None:
            self.mc.replace(item, value, flags, exptime)
            return self.STORED
        else:
            return self.NOT_STORED

    def prepend(self, key, flags, exptime, value):
        item = self.mc.get(key)
        if item is None:
            return self.NOT_STORED
        else:
            value = value + item.value
            self.mc.replace(item, value, flags, exptime)
            return self.STORED

    def append(self, key, flags, exptime, value):
        item = self.mc.get(key)
        if item is None:
            return self.NOT_STORED
        else:
            value = item.value + value
            self.mc.replace(item, value, flags, exptime)
            return self.STORED

    def increment(self, key, value):
        item = self.mc.get(key)
        if item is not None:
            if not item.value.isdigit():
                return (self.NOT_NUMBER, None)
            else:
                value = str(int(item.value) + int(value))
                item = self.mc.replace(item, value)
                self._stats.incr(True)
                return (self.STORED, item.value)
        else:
            self._stats.incr(False)
            return (self.NOT_FOUND, None)

    def decrement(self, key, value):
        item = self.mc.get(key)
        if item is not None:
            if not item.value.isdigit():
                return (self.NOT_NUMBER, None)
            else:
                value = str(int(item.value) - int(value))
                item = self.mc.replace(item, value)
                self._stats.decr(True)
                return (self.STORED, item.value)
        else:
            self._stats.decr(False)
            return (self.NOT_FOUND, None)

    def get(self, keys):
        items = [(key, self.mc.get(key)) for key in keys]
        items = [(key, item.value, item.flags) 
                 for key, item in items if item is not None]
        self._stats.get(bool(items))
        return items

    def gets(self, keys):
        items = [(key, self.mc.get(key)) for key in keys]
        items = [(key, item.value, item.flags, item.casunique()) 
                  for key, item in items if item is not None]
        self._stats.get(bool(items))
        return items

    def delete(self, key):
        item = self.mc.get(key)
        if item is not None:
            self.mc.delete(item)
            self._stats.delete(True)
            return self.DELETED
        else:
            self._stats.delete(False)
            return self.NOT_FOUND

    def flush(self, delay):
        self.mc.flush(delay)

    def stats(self, sub):
        stats = self._stats.dump(sub)
        return stats
