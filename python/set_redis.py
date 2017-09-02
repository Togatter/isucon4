# -*- coding: utf-8 -*-
import copy
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

def init_dict_index(index, dicts):
    if index not in dicts:
        dicts[index] = 0

    return dicts

def update_last_login(users_login_info, login_log):
    # db = connect_db()
    # cur = db.cursor()
    # cur.execute(
    #     "SELECT * FROM last_login_log WHERE user_id = {} FOR UPDATE".format(login_log['user_id'])
    # )
    # last_login = cur.fetchone()
    # if last_login:
    #     cur.execute(
    #         "UPDATE last_login_log SET last_at = '{}', now_at = '{}', ip = '{}', last_ip = '{}' WHERE user_id = {}".format(
    #             last_login['now_at'].strftime("%Y-%m-%d %H:%M:%S"),
    #             login_log['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
    #             login_log['ip'],
    #             last_login['ip'],
    #             login_log['user_id'],
    #         )
    #     )
    #
    # else:
    #     cur.execute(
    #         "INSERT INTO last_login_log (`now_at`, `last_at`, `user_id`, `login`, `ip`, `last_ip`) VALUES ('{}', '{}', {}, '{}', '{}', '{}')".format(
    #             login_log['created_at'],
    #             login_log['created_at'],
    #             login_log['user_id'],
    #             login_log['login'],
    #             login_log['ip'],
    #             login_log['ip'],
    #         )
    #     )
    # cur.close()
    # db.commit()

    user_id = str(login_log['user_id'])
    if users_login_info[user_id] == 0:
        users_login_info[user_id] = {
            'now_at': login_log['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
            'last_at': login_log['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
            'user_id': login_log['user_id'],
            'login': login_log['login'],
            'ip': login_log['ip'],
            'last_ip': login_log['ip'],
        }
    else:
        user_login_info = copy.deepcopy(users_login_info[user_id])
        users_login_info[user_id] = {
            'now_at': login_log['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
            'last_at': user_login_info['now'],
            'user_id': login_log['user_id'],
            'login': login_log['login'],
            'ip': login_log['ip'],
            'last_ip': user_login_info['ip'],
        }

    return users_login_info

def init_redis():

    redis_client.flushall()

    cur = connect_db().cursor()
    cur.execute(
        "SELECT * FROM login_log ORDER BY id"
    )
    users_fail_count = {}
    ips_fail_count = {}
    users_login_info = {}
    for row in cur.fetchall():

        if row['user_id']:
            user_id = str(row['user_id'])
            users_fail_count = init_dict_index(user_id, users_fail_count)
            users_login_info = init_dict_index(user_id, users_login_info)
            if row['succeeded']:
                users_fail_count[user_id] = 0
                users_login_info = update_last_login(users_login_info, row)
            else:
                users_fail_count[user_id] += 1

        ip = row['ip']
        ips_fail_count = init_dict_index(ip, ips_fail_count)
        if row['succeeded']:
            ips_fail_count[ip] = 0
        else:
            ips_fail_count[ip] += 1
    cur.close()

    with redis_client.pipeline() as pipe:
        redis_client.set_user_count(users_fail_count, pipe)
        redis_client.set_ip_count(ips_fail_count, pipe)

    for row in  users_login_info.iteritems():
        db = connect_db()
        cur = db.cursor()

        cur.execute(
        "INSERT INTO last_login_log (`now_at`, `last_at`, `user_id`, `login`, `ip`, `last_ip`) VALUES ('{}', '{}', {}, '{}', '{}', '{}')".format(
            row['now_at'],
            row['last_at'],
            row['user_id'],
            row['login'],
            row['ip'],
            row['last_ip'],
        )
        )

        cur.close()
        db.commit()
if __name__ == '__main__':
    init_redis()
