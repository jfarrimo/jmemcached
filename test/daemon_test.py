#!/usr/local/bin/python
import memcache

mc = memcache.Client(['127.0.0.1:11211'], debug=False)
#mc = memcache.Client(['192.168.1.94:11211'], debug=False)

# SET
######

mc.set('set_key', 'set value')
value = mc.get('set_key')
print '%s' % value

# REPLACE
##########

mc.replace('repl_key', 'no prev value')
value = mc.get('repl_key')
print '%s' % value

mc.set('repl_key2', 'prev repl value')
mc.replace('repl_key2', 'new prev repl value')
value = mc.get('repl_key2')
print '%s' % value

# GET
######

mc.set('get_key', 'get value')
value = mc.get('get_key')
print '%s' % value

value = mc.get('none_key')
print '%r' % value

# DELETE
#########

mc.delete('del_key')
value = mc.get('del_key')
print '%r' % value

mc.delete('none_key')
