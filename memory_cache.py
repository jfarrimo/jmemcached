"""
Memcached engine.

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
import sys

import memory_cache_primitives

class MemcachedStats(memory_cache_primitives.MemoryCacheStats):
    """ stats for memcached """
    # pylint: disable=R0902
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
    # pylint: enable=R0902

    def set(self):
        """ set command """
        self.cmd_set += 1

    def get(self, hit):
        """ get command """
        self.cmd_get += 1
        if hit:
            self.get_hits += 1
        else:
            self.get_misses += 1

    def delete(self, hit):
        """ delete command """
        if hit:
            self.delete_hits += 1
        else:
            self.delete_misses += 1

    def incr(self, hit):
        """ incr command """
        if hit:
            self.incr_hits += 1
        else:
            self.incr_misses += 1

    def decr(self, hit):
        """ decr command """
        if hit:
            self.decr_hits += 1
        else:
            self.decr_misses += 1

    def cas_miss(self):
        """ cas miss """
        self.cas_misses += 1

    def cas_hit(self):
        """ cas hit """
        self.cas_hits += 1

    def cas_badval(self):
        """ cas badval """
        self.cas_badvals += 1

    def dump(self, command):
        """ dump the contents """
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
    """
    memcached engine
    """
    DELETED = 0
    EXISTS = 1
    NOT_FOUND = 2
    NOT_NUMBER = 3
    NOT_STORED = 4
    STORED = 5

    def __init__(self, stats, max_items=DEFAULT_MAX_ITEMS, 
                 max_bytes=DEFAULT_MAX_BYTES):
        self._stats = stats
        self.cache = memory_cache_primitives.MemoryCache(
            self._stats, max_items, max_bytes)

    def set(self, key, flags, exptime, value):
        """ set command """
        item = self.cache.get(key)
        if item is not None:
            self.cache.replace(item, value, flags, exptime)
        else:
            self.cache.add(key, value, flags, exptime)
        self._stats.set()
        return self.STORED

    # pylint: disable=R0913
    def cas(self, key, flags, exptime, casunique, value):
        """ cas command """
        item = self.cache.get(key)
        if item is None:
            self.cache.add(key, value, flags, exptime)
            self._stats.cas_miss()
            return self.NOT_FOUND
        elif item.casunique() == int(casunique):
            self.cache.replace(item, value, flags, exptime)
            self._stats.cas_hit()
            return self.STORED
        else:
            self._stats.cas_badval()
            return self.EXISTS
    # pylint: enable=R0913

    def add(self, key, flags, exptime, value):
        """ add command """
        item = self.cache.get(key)
        if item is not None:
            self.cache.touch(item)
            return self.NOT_STORED
        else:
            self.cache.add(key, value, flags, exptime)
            return self.STORED

    def replace(self, key, flags, exptime, value):
        """ replace command """
        item = self.cache.get(key)
        if item is not None:
            self.cache.replace(item, value, flags, exptime)
            return self.STORED
        else:
            return self.NOT_STORED

    def prepend(self, key, flags, exptime, value):
        """ prepend command """
        item = self.cache.get(key)
        if item is None:
            return self.NOT_STORED
        else:
            value = value + item.value
            self.cache.replace(item, value, flags, exptime)
            return self.STORED

    def append(self, key, flags, exptime, value):
        """ append command """
        item = self.cache.get(key)
        if item is None:
            return self.NOT_STORED
        else:
            value = item.value + value
            self.cache.replace(item, value, flags, exptime)
            return self.STORED

    def increment(self, key, value):
        """ increment command """
        item = self.cache.get(key)
        if item is not None:
            if not item.value.isdigit():
                return (self.NOT_NUMBER, None)
            else:
                value = str(int(item.value) + int(value))
                item = self.cache.replace(item, value)
                self._stats.incr(True)
                return (self.STORED, item.value)
        else:
            self._stats.incr(False)
            return (self.NOT_FOUND, None)

    def decrement(self, key, value):
        """ decrement command """
        item = self.cache.get(key)
        if item is not None:
            if not item.value.isdigit():
                return (self.NOT_NUMBER, None)
            else:
                value = str(int(item.value) - int(value))
                item = self.cache.replace(item, value)
                self._stats.decr(True)
                return (self.STORED, item.value)
        else:
            self._stats.decr(False)
            return (self.NOT_FOUND, None)

    def get(self, keys):
        """ get command """
        items = [(key, self.cache.get(key)) for key in keys]
        items = [(key, item.value, item.flags) 
                 for key, item in items if item is not None]
        self._stats.get(bool(items))
        return items

    def gets(self, keys):
        """ gets command """
        items = [(key, self.cache.get(key)) for key in keys]
        items = [(key, item.value, item.flags, item.casunique()) 
                  for key, item in items if item is not None]
        self._stats.get(bool(items))
        return items

    def delete(self, key):
        """ delete command """
        item = self.cache.get(key)
        if item is not None:
            self.cache.delete(item)
            self._stats.delete(True)
            return self.DELETED
        else:
            self._stats.delete(False)
            return self.NOT_FOUND

    def flush(self, delay):
        """ flush command """
        self.cache.flush(delay)

    def stats(self, sub):
        """ stats command """
        stats = self._stats.dump(sub)
        return stats
