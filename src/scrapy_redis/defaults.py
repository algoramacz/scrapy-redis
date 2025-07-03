import redis


# For standalone use.
DUPEFILTER_KEY = 'dupefilter:%(timestamp)s'

PIPELINE_KEY = '%(spider)s:items'

STATS_KEY = '%(spider)s:stats'

REDIS_CLS = redis.StrictRedis
REDIS_ENCODING = 'utf-8'
# Sane connection defaults.
REDIS_PARAMS = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'encoding': REDIS_ENCODING,
}
REDIS_CONCURRENT_REQUESTS = 16

SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.PriorityQueue'
SCHEDULER_REQUESTS_KEY = None
SCHEDULER_QUEUE_KEY = None
SCHEDULER_QUEUE_LENGTH_KEY = None
SCHEDULER_DUPEFILTER_KEY = '%(spider)s:dupefilter'
SCHEDULER_DUPEFILTER_CLASS = 'scrapy_redis.dupefilter.RFPDupeFilter'
SCHEDULER_PERSIST = False
START_URLS_KEY = '%(name)s:start_urls'
START_URLS_AS_SET = False
START_URLS_AS_ZSET = False
MAX_IDLE_TIME = 0

import time
from functools import wraps
import logging

logger = logging.getLogger(__name__)

def timeit(method):
    @wraps(method)
    def timed(*args, **kw):
        start_time = time.time()
        result = method(*args, **kw)
        end_time = time.time()
        
        logger.info(f'{method.__name__} took {(end_time - start_time) * 1000} ms')
        return result
    
    return timed
