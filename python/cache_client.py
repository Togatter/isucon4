# -*- coding: utf-8 -*-
import abc
import os
import redis


CACHE_SERVER_HOST = os.environ.get('CACHE_SERVER_HOST', 'localhost')
CACHE_SERVER_PORT = os.environ.get('CACHE_SERVER_PORT', 6379)
CACHE_SERVER_DB = os.environ.get('CACHE_SERVER_DB', 0)

class BaseClient(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get(self, key):
        pass

    def set(self, key, value):
        pass

    def lock(self):
        pass

class RedisClient(BaseClient):

    def __init__(self, *args, **kwargs):

        host = kwargs.get('host', CACHE_SERVER_HOST)
        port = kwargs.get('port', CACHE_SERVER_PORT)
        db = kwargs.get('db', CACHE_SERVER_DB)
        self.client = redis.Redis(host=host, port=port, db=db)

    def get(self, key):
        return self.client.get(key)

    def set(self, key, value):
        self.client.set(self._key(key), value)

    def delete(self, key):
        self.client.delete(self._key(key))

    def get_ip_count(self, value):
        return int(self.client.zscore(self._ip_count_key, value) or 0)

    def set_ip_count(self, dicts, pipe=None):
        client = pipe if pipe else self.client
        client.zadd(self._ip_count_key, **dicts)

    def ip_count_inc(self, value, num):
        return self.client.zincrby(self._ip_count_key , value, num)

    def ip_count_reset(self, value):
        self.client.zrem(self._ip_count_key, value)

    def get_user_count(self, value):
        return int(self.client.zscore(self._user_count_key, value) or 0)

    def set_user_count(self, dicts, pipe=None):
        client = pipe if pipe else self.client
        client.zadd(self._user_count_key, **dicts)

    def user_count_inc(self, value, num):
        return self.client.zincrby(self._user_count_key , value, num)

    def user_count_reset(self, value):
        self.client.zrem(self._user_count_key, value)

    def _key(self, key):
        if key is None:
            raise
        return "Redis::{}::{}::{}".format(self.__module__, self.__class__.__name__, key)

    def pipeline(self, transaction=True):
        return self.client.pipeline(transaction=transaction)

    @property
    def _ip_count_key(self):
        return "Redis::{}::{}::{}".format(self.__module__, self.__class__.__name__, "ip_count")

    @property
    def _user_count_key(self):
        return "Redis::{}::{}::{}".format(self.__module__, self.__class__.__name__, "user_count")

class CacheClient(object):

    class Type(object):
        REDIS = 0
        MEMCACHED = 1

    Classes = {
        Type.REDIS: RedisClient,
    }

    @classmethod
    def get(cls, type=Type.REDIS, *args, **kwargs):
        return cls.Classes[type](*args, **kwargs)
