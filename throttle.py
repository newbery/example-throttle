import logging
from itertools import izip_longest
from time import time, sleep

from decorator import decorator

# Warning: The simplecache doesn't support the expected lock interface
# so make sure to use a lockable cache.
# See https://pypi.org/project/redis for an example.
from .cache import CACHE, cachekey_static

try:
    from django.conf import settings
    THROTTLE_ENABLED = getattr(settings, 'THROTTLE_ENABLED', True)
    THROTTLE_LOGGING = getattr(settings, 'THROTTLE_LOGGING', True)
    LOGGER_PREFIX = getattr(settings, 'LOGGER_PREFIX', True)
except ImportError:
    THROTTLE_ENABLED = True
    THROTTLE_LOGGING = False
    LOGGER_PREFIX = ''

if THROTTLE_LOGGING:
    logger = logging.getLogger(LOGGER_PREFIX + __name__)

    def log_message(label, text, seconds):
        if isinstance(label, (tuple, list)):
            label = str([i[1] for i in label])
        return '"%s" throttle %s %s seconds' % (label, text, seconds)

    def log_info(label, text, seconds):
        logger.info(log_message(label, text, seconds))
        
    def log_warning(*args, **kwargs):
        logger.warning(*args, **kwargs)

else:
    def log_info(*args, **kwargs):
        pass
    log_warning = log_info


class ThrottleTimeout(Exception):
    pass


def throttle(limit, key=cachekey_static, cache=CACHE,
             retry=True, timeout=None, marker=None, lockargs=None):
    """
    A decorator to ensure function calls are rate-limited. Calls exceeding
    limit are dropped or retried until limit condition is satisfied.

    Multiple rate limits can be set by passing in a tuple or list as values
    for `limit` and `key`.  The limits are tested in order so it's usually
    best to list the most likely limits first... assuming all other criteria
    is roughly equal, this will likely mean from the smallest to largest
    limit value (or from slowest to fastest rate).

    `limit`
        Maximum rate in calls/second
    `key`
        Cache key function to rate-limit calls into distinct buckets
        (default: static_cachekey)
    `cache`
        Cache object with django cache style  `set` and `get` interface
        (default: django_cache )
    `retry`
        If True, retry until rate-limit condition is satisfied or until timeout
        (default: True)
    `timeout`
        Maximum time limit before next retry attempt should just raise an error
        (default: the greater of 10 seconds or 10/limit)
    `marker`
        Object returned when call is rate-limited and `retry` is False
        (default: None)
    `lockargs`
        A dictionary to override the default kwargs for `cache.lock` function.
        (default: None)
    """
    if not THROTTLE_ENABLED:
        return lambda func: func

    _timeout = timeout or 10
    multi = isinstance(limit, (tuple, list))
    if multi:
        if not isinstance(key, (tuple, list)):
            key = [key] if key else []
        assert len(limit) >= len(key)
        minimum = [1.0 / float(l) for l in limit]
        maximum = max(minimum)
        expire = [max(10 * m, _timeout) for m in minimum]
        limit = list(izip_longest(
            minimum, key, expire, fillvalue=cachekey_static))
    else:
        minimum = maximum = 1.0 / float(limit)
        expire = max(10 * minimum, _timeout)

    timeout = timeout or max(10, maximum * 10)
    lockargs = lockargs or dict(timeout=1, blocking_timeout=timeout)

    def _message(label, text, seconds):
        if multi:
            label = str([i[1] for i in label])
        return '"%s" throttle %s %s seconds' % (label, text, seconds)

    def _now(label, start):
        now = time()
        if now - start > timeout:
            message = log_message(label, 'timeout after', now - start)
            log_warning(message)
            raise ThrottleTimeout(message)
        return now

    @decorator
    def single_limit(func, *args, **kwargs):
        _key = key(func, args, kwargs)

        if _key:
            start = time()
            done = False

            while not done:
                delay = 0
                done = True
                with cache.lock('throttle.lock', **lockargs):
                    now = _now(_key, start)
                    delay = max(cache.get(_key, 0) + minimum - now, 0)
                    if not delay:
                        cache.set(_key, now, expire)
                if delay:
                    if not retry:
                        return marker
                    log_info(_key, 'retry in', delay)
                    sleep(delay)
                    done = False

        return func(*args, **kwargs)

    @decorator
    def multi_limit(func, *args, **kwargs):
        _limits = [
            (minimum, key(func, args, kwargs), expire)
            for minimum, key, expire in limit]
        _limits = [
            (minimum, key, expire)
            for minimum, key, expire in _limits if key]

        if _limits:
            start = time()
            done = False

            while not done:
                delay = 0
                done = True
                with cache.lock('throttle.lock', **lockargs):
                    now = _now(_limits, start)
                    seen = set()
                    for minimum, key, expire in _limits:
                        if key in seen:
                            continue
                        seen.add(key)
                        delay = max(cache.get(key, 0) + minimum - now, 0)
                        if delay:
                            break
                        cache.set(key, now, expire)
                if delay:
                    if not retry:
                        return marker
                    log_info(_limits, 'retry in', delay)
                    sleep(delay)
                    done = False

        return func(*args, **kwargs)

    return multi_limit if multi else single_limit
