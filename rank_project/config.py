# -*- coding:utf-8 -*-

import redis
from rediscluster import RedisCluster

ZMQ_VENT_PORT = '6387'
ZMQ_CTRL_VENT_PORT = '5585'
ZMQ_VENT_HOST = '10.168.57.141'
ZMQ_CTRL_HOST = '10.168.57.141'

BIN_FILE_PATH = '../weibo'

REDIS_VENT_HOST = '10.168.57.141'
REDIS_VENT_PORT = '6379'
REDIS_NICK_UID_HOST = '10.168.57.151'  
REDIS_NICK_UID_PORT = '6379'

NICK_UID_FILE_PATH = '../nickname-id-STDB.txt'
NICK_UID_NAMESPACE = 'nick_id'
ACTIVE_NICK_UID_NAMESPACE = "active_nick_id"

def _default_cluster_redis(host=REDIS_VENT_HOST, port=REDIS_VENT_PORT):
    startup_nodes = [{"host": host, "port": port}]
    weibo_redis = RedisCluster(startup_nodes = startup_nodes)
    return weibo_redis

def _default_single_redis(host, port):
    r = redis.StrictRedis(host=REDIS_NICK_UID_HOST, port=REDIS_NICK_UID_PORT)
    
    return r
