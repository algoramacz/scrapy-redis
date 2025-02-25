import threading
import time
from redis import WatchError
import atexit

try:
    from scrapy.utils.request import request_from_dict
except ImportError:
    from scrapy.utils.reqser import request_to_dict, request_from_dict

from . import picklecompat
#from .defaults import timeit

class Base(object):
    """Per-spider base queue class"""

    def __init__(self, server, 
                       spider, 
                       key, 
                       length_key, 
                       serializer=None,
                       push_buffer_size=100,
                       push_flush_interval=2):
        """Initialize per-spider redis queue.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        spider : Spider
            Scrapy spider instance.
        key: str
            Redis key where to put and get messages.
        serializer : object
            Serializer object with ``loads`` and ``dumps`` methods.

        """
        if serializer is None:
            # Backward compatibility.
            # TODO: deprecate pickle.
            serializer = picklecompat
        if not hasattr(serializer, 'loads'):
            raise TypeError(f"serializer does not implement 'loads' function: {serializer}")
        if not hasattr(serializer, 'dumps'):
            raise TypeError(f"serializer does not implement 'dumps' function: {serializer}")

        self.server = server
        self.spider = spider
        self.key = key % {'spider': spider.name}
        self.length_key = length_key % {'spider': spider.name}
        self.serializer = serializer

        self.push_buffer_size = push_buffer_size
        self.push_flush_interval = push_flush_interval
        self.push_buffer = []
        self.push_running = True
        self.push_buffer_lock = threading.Lock()    
        self.push_flush_thread = threading.Thread(target=self.flush_push_buffer_periodically, daemon=True)
        self.push_flush_thread.start()

        atexit.register(self.shutdown)

    def _encode_request(self, request):
        """Encode a request object"""
        try:
            obj = request.to_dict(spider=self.spider)
        except AttributeError:
            obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, spider=self.spider)

    def __len__(self):
        """Return the length of the queue"""
        count = self.server.get(self.length_key)
        return int(count) if count else 0

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, queue_key, timeout=0):
        """Pop a request"""
        raise NotImplementedError
    
    def flush_push_buffer(self):
        raise NotImplementedError
    
    def flush_push_buffer_periodically(self):
        while self.push_running:
            time.sleep(self.push_flush_interval)
            self.flush_push_buffer()

    def clear(self, crawl_id):
        """Clear queue/stack"""
        if crawl_id is None:
            return False
        
        self.server.delete(self.dequeue_key(crawl_id))
        return True

    def enqueue_key(self, request):
        """Return a key to identify the queue"""

        crawl_id = request.meta.get('crawl_id', None)
        return f"{self.key}:{crawl_id}"
    
    def dequeue_key(self, crawl_id):
        """Return a key to identify the queue"""
        return f"{self.key}:{crawl_id}"
    

    def shutdown(self):
        """Handle the shutdown process."""
        self.push_running = False
        self.push_flush_thread.join()
        self.flush_push_buffer()


    #@timeit
    def queues_lengths(self):
        lua_script = """
            local lengths = {}
            for i, key in ipairs(KEYS) do
                lengths[#lengths+1] = redis.call('ZCARD', key)
            end
            return lengths
            """

        # Use scan_iter to get the keys safely without blocking the server
        keys = [key for key in self.server.scan_iter(f"{self.key}:*")]

        # You may want to process these in batches if there are a lot of keys
        batch_size = 1000  # Number of keys to process in each batch
        queues_lengths = {}

        for i in range(0, len(keys), batch_size):
            batch_keys = keys[i:i+batch_size]
            lengths = self.server.eval(lua_script, len(batch_keys), *batch_keys)
            queues_lengths.update(dict(zip(batch_keys, lengths)))

        # At this point, queue_lengths contains all your queues and their sizes

        queues_lengths = {k.decode('utf-8'): v for k, v in queues_lengths.items()}
        return queues_lengths


    def reconcile_queue_length(self):
        lua_script = f"""
            local total_count = 0
            local keys = redis.call('KEYS', '{self.key}:*')
            for _, key in ipairs(keys) do
                total_count = total_count + redis.call('ZCARD', key)
            end
            redis.call('SET', '{self.length_key}', total_count)
            return total_count
        """

        total_count = self.server.eval(lua_script, 0) 
        return total_count

class PriorityQueue(Base):
    """Per-spider priority queue abstraction using redis' sorted set"""

    #@timeit
    def push(self, request):
        """Push a request"""
        data = self._encode_request(request)
        score = -request.priority
        # We don't use zadd method as the order of arguments change depending on
        # whether the class is Redis or StrictRedis, and the option of using
        # kwargs only accepts strings, not bytes.

        with self.push_buffer_lock:
            self.push_buffer.append((self.enqueue_key(request), {data: score}))

            # if len(self.push_buffer) >= self.push_buffer_size:
            #     self.flush_push_buffer()

        # with self.server.pipeline() as pipe:
        #     while True:
        #         try:
        #             pipe.watch(self.length_key)
        #             pipe.multi()
        #             pipe.zadd(self.enqueue_key(request), {data: score})
        #             pipe.incr(self.length_key)
        #             pipe.execute()
        #             break
        #         except WatchError:
        #             continue

    #@timeit 
    def flush_push_buffer(self):
        """Flush the push buffer to Redis"""
        with self.push_buffer_lock:
            if not self.push_buffer:
                return

            with self.server.pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(self.length_key)
                        pipe.multi()
                        for key, data in self.push_buffer:
                            pipe.zadd(key, data)
                        pipe.incrby(self.length_key, len(self.push_buffer))
                        pipe.execute()
                        break
                    except WatchError:
                        continue

            self.push_buffer = []



    #@timeit
    def pop(self, crawl_id, timeout=0):
        """
        Pop a request
        timeout not support in this queue class
        """
        # use atomic range/remove using multi/exec

        queue_key = self.dequeue_key(crawl_id)
        
        with self.server.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(queue_key)
                    # Begin the transaction
                    pipe.multi()
                    # Get the first item in the zset and remove it atomically
                    pipe.zrange(queue_key, 0, 0)
                    pipe.zremrangebyrank(queue_key, 0, 0)
                    results, removed = pipe.execute()[:2]
                    
                    # If an item was removed, decrement the length key
                    if removed:
                        self.server.decr(self.length_key)
                    
                    # If we got a result, return the decoded request
                    if results:
                        return self._decode_request(results[0])
                    
                    return None  # No item was popped
                except WatchError:
                    continue

class FifoQueue(Base):
    """Per-spider FIFO queue"""

    def push(self, request):
        """Push a request"""
        raise NotImplementedError
        self.server.lpush(self.enqueue_key(request), self._encode_request(request))

    def pop(self, queue_key, timeout=0):
        """Pop a request"""
        raise NotImplementedError        
        if timeout > 0:
            data = self.server.brpop(queue_key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.rpop(queue_key)
        if data:
            return self._decode_request(data)

class LifoQueue(Base):
    """Per-spider LIFO queue."""

    def push(self, request):
        """Push a request"""
        raise NotImplementedError        
        self.server.lpush(self.enqueue_key(request), self._encode_request(request))

    def pop(self, queue_key, timeout=0):
        """Pop a request"""
        raise NotImplementedError        
        if timeout > 0:
            data = self.server.blpop(queue_key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.lpop(queue_key)

        if data:
            return self._decode_request(data)


# TODO: Deprecate the use of these names.
SpiderQueue = FifoQueue
SpiderStack = LifoQueue
SpiderPriorityQueue = PriorityQueue
