from typing import Callable

import importlib
import six

from scrapy.utils.misc import load_object

from . import connection, defaults
#from .defaults import timeit


# TODO: add SCRAPY_JOB support.
class Scheduler(object):
    """Redis-based scheduler

    Settings
    --------
    SCHEDULER_PERSIST : bool (default: False)
        Whether to persist or clear redis queue.
    SCHEDULER_FLUSH_ON_START : bool (default: False)
        Whether to flush redis queue on start.
    SCHEDULER_IDLE_BEFORE_CLOSE : int (default: 0)
        How many seconds to wait before closing if no message is received.
    SCHEDULER_QUEUE_KEY : str
        Scheduler redis key.
    SCHEDULER_QUEUE_CLASS : str
        Scheduler queue class.
    SCHEDULER_DUPEFILTER_KEY : str
        Scheduler dupefilter redis key.
    SCHEDULER_DUPEFILTER_CLASS : str
        Scheduler dupefilter class.

    """

    def __init__(self, server,
                 requests_key=defaults.SCHEDULER_REQUESTS_KEY,
                 queue_key=defaults.SCHEDULER_QUEUE_KEY,
                 queue_cls=defaults.SCHEDULER_QUEUE_CLASS,
                 queue_length_key=defaults.SCHEDULER_QUEUE_LENGTH_KEY,
                 dupefilter_key=defaults.SCHEDULER_DUPEFILTER_KEY,
                 dupefilter_cls=defaults.SCHEDULER_DUPEFILTER_CLASS,
                 idle_before_close=0):
        """Initialize scheduler.

        Parameters
        ----------
        server : Redis
            The redis server instance.
        queue_key : str
            Requests queue key.
        queue_cls : str
            Importable path to the queue class.
        dupefilter_key : str
            Duplicates filter key.
        dupefilter_cls : str
            Importable path to the dupefilter class.
        idle_before_close : int
            Timeout before giving up.

        """
        if idle_before_close < 0:
            raise TypeError("idle_before_close cannot be negative")

        self.server = server
        self.requests_key = requests_key
        self.queue_key = queue_key
        self.queue_cls = queue_cls
        self.queue_length_key = queue_length_key
        self.dupefilter_cls = dupefilter_cls
        self.dupefilter_key = dupefilter_key
        self.idle_before_close = idle_before_close
        self.request_fingerprint: Callable = None
        self.stats = None

    def __len__(self):
        return len(self.queue)

    @classmethod
    def from_settings(cls, settings):
        kwargs = {
            'idle_before_close': settings.getint('SCHEDULER_IDLE_BEFORE_CLOSE'),
        }

        # If these values are missing, it means we want to use the defaults.
        optional = {
            # TODO: Use custom prefixes for this settings to note that are
            # specific to scrapy-redis.
            'queue_cls': 'SCHEDULER_QUEUE_CLASS',
            'requests_key': 'SCHEDULER_REQUESTS_KEY',
            'queue_key': 'SCHEDULER_QUEUE_KEY',
            'queue_length_key': 'SCHEDULER_QUEUE_LENGTH_KEY',
            'dupefilter_key': 'SCHEDULER_DUPEFILTER_KEY',
            # We use the default setting name to keep compatibility.
            'dupefilter_cls': 'DUPEFILTER_CLASS',
        }
        for name, setting_name in optional.items():
            val = settings.get(setting_name)
            if val:
                kwargs[name] = val

        server = connection.from_settings(settings)
        # Ensure the connection is working.
        server.ping()

        return cls(server=server, **kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls.from_settings(crawler.settings)
        # FIXME: for now, stats are only supported from this constructor
        instance.stats = crawler.stats
        return instance

    def open(self, spider):
        self.spider = spider

        self.df = load_object(self.dupefilter_cls).from_spider(spider)
        self.request_fingerprint = self.df.request_fingerprint

        try:
            self.queue = load_object(self.queue_cls)(
                server=self.server,
                spider=spider,
                request_fingerprint=self.request_fingerprint,
            )
        except TypeError as e:
            raise ValueError(f"Failed to instantiate queue class '{self.queue_cls}': {e}")


        # make sure total len corresponds to queue lengths 
        self.queue.reconcile_queue_length()

        # notice if there are requests already in the queue to resume the crawl
        if len(self.queue):
            spider.log(f"Resuming crawl ({len(self.queue)} requests scheduled)")

    def close(self, reason):
        self.flush()

    def flush(self):
        self.queue.flush_buffers()

    #@timeit
    def enqueue_request(self, request):
        if self.df.request_seen(request) and not request.dont_filter:
            self.df.log(request, self.spider)
            return False
        if self.stats:
            self.stats.inc_value('scheduler/enqueued/redis', spider=self.spider)
        self.queue.push(request)
        return True

    #@timeit
    def next_request(self):
        block_pop_timeout = self.idle_before_close

        crawl_id = None
        if callable(self.spider.next_request_crawl_id_callback):
            crawl_id = self.spider.next_request_crawl_id_callback()
        request = self.queue.pop(crawl_id, block_pop_timeout)
        if request and self.stats:
            self.stats.inc_value('scheduler/dequeued/redis', spider=self.spider)
        return request

    #@timeit
    def has_pending_requests(self):
        return len(self) > 0
