#!/usr/local/bin/python
import memcache
import time

BATCH_SIZE = 1000

#mc = memcache.Client(['127.0.0.1:11211'], debug=False)
mc = memcache.Client(['192.168.1.94:11212'], debug=False)

total_sets = 0
while(True):
    start_time = time.time()
    for i in xrange(BATCH_SIZE):
        mc.set('set_key_%s' % total_sets, 'set value %s' % total_sets)
        total_sets += 1
    total_time = time.time() - start_time
    print '%s time, %s batch size, %s total sets' % (total_time, BATCH_SIZE, total_sets)
