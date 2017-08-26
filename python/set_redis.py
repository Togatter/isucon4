# -*- coding: utf-8 -*-
import os
import MySQLdb
from MySQLdb.cursors import DictCursor
from cache_client import CacheClient

redis_client = CacheClient.get()

def connect_db():
    host = os.environ.get('ISU4_DB_HOST', 'localhost')
    port = int(os.environ.get('ISU4_DB_PORT', '3306'))
    dbname = os.environ.get('ISU4_DB_NAME', 'isu4_qualifier')
    username = os.environ.get('ISU4_DB_USER', 'root')
    password = os.environ.get('ISU4_DB_PASSWORD', '')
    db = MySQLdb.connect(host=host, port=port, db=dbname, user=username, passwd=password, cursorclass=DictCursor, charset='utf8')
    return db

def init_dict_index(index, dict):
    if index not in dict:
        dict[index] = 0

    return dict

def init_redis():
    cur = connect_db().cursor()
    cur.execute(
        "SELECT * FROM login_log ORDER BY created_at"
    )
    users_fail_count = {}
    ips_fail_count = {}
    for row in cur.fetchall():

        if 'user_id' in row:
            user_id = row['user_id']
            users_fail_count = init_dict_index(user_id, users_fail_count)
            if row['succeeded']:
                users_fail_count[user_id] = 0
            else:
                users_fail_count[user_id] += 1

        ip = row['ip']
        ips_fail_count = init_dict_index(ip, ips_fail_count)
        if row['succeeded']:
            ips_fail_count = 0
        else:
            ips_fail_count += 1
    cur.close()

    with redis_client.pipeline() as pipe:
        redis_client.set_user_count(users_fail_count, pipe)
        redis_client.set_ip_count(ips_fail_count, pipe)

if __name__ == '__main__':
    init_redis()
