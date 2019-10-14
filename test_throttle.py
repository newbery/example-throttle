import unittest
from time import time

# TODO: Fix this to remove the dependancy on the `redis` library
from redis.lock import Lock, LockError

from .simplecache import Cache
from .throttle import throttle, ThrottleTimeout


class MockLock(Lock):

    def do_acquire(self, token):
        if self.redis.blocked:
            return False
        return self.redis._cache.setdefault(self.name, token) == token

    def do_release(self, expected_token):
        if self.redis._cache.get(self.name) == expected_token:
            del self.redis._cache[self.name]
        else:
            raise LockError("Cannot release a lock that's no longer owned")


class MockLockableCache(Cache):
    blocked = False
    limited = False

    def get(self, key, default=None):
        if self.limited:
            return time()
        return self._cache.get(self.make_key(key), default)

    def lock(self, key, version=None, timeout=None, sleep=0.1,
             blocking_timeout=None):
        name = self.make_key(key, version=version)
        return MockLock(
            self, name, timeout=timeout, sleep=sleep,
            blocking_timeout=blocking_timeout)

    def reset(self):
        self.limited = self.blocked = False
        self._cache = {}


mock_cache = MockLockableCache()


def measure_drift(func, count=1, blocked=False, limited=False):
    mock_cache.reset()
    mock_cache.blocked = blocked
    mock_cache.limited = limited
    expected = func() * count
    start = time()
    while count:
        count -= 1
        func()
    return expected, time() - start


class TestThrottle(unittest.TestCase):
    """
    Test `lib.throttle.throttle` decorator.
    """

    def assertNotDrifted(self, expected, real):
        self.assertTrue(real > 0)
        self.assertAlmostEqual(expected, real, delta=expected / 10)

    def test_throttle(self):
        """
        A throttled function should be delayed by an expected amount,
        within a reasonable margin of error (< 10% for very fast rates)
        """

        @throttle(10, cache=mock_cache)
        def func():
            return 0.1

        expected, real = measure_drift(func, count=3)
        self.assertNotDrifted(expected, real)

    def test_throttle_multi(self):
        """
        A function with multiple rate limits should be throttled to
        the slowest rate.  In this example, both rates are sharing the same
        static key so only the slowest of the two matters.
        """

        @throttle([10, 100], cache=mock_cache)
        def func():
            return 0.1

        expected, real = measure_drift(func, count=3)
        self.assertNotDrifted(expected, real)

    def test_throttle_blocked(self):
        """
        A throttle that is unable to obtain a cache lock within the timeout
        should throw an error.
        """

        @throttle(10, cache=mock_cache, timeout=.5)
        def func():
            return 0.1

        with self.assertRaises(LockError):
            measure_drift(func, blocked=True)

    def test_throttle_limited(self):
        """
        A throttle that is always rate limited should throw an error.
        """

        @throttle(10, cache=mock_cache, timeout=.5)
        def func():
            return 0.1

        with self.assertRaises(ThrottleTimeout):
            measure_drift(func, limited=True)
