#!/usr/bin/python
import MySQLdb
import sys
import os.path
from os import stat
import time
import sqlite3
import shutil
import logging
# ------mysql info------
INDEX_HOST = "users.host"
INDEX_DB = "users.database"
INDEX_USER = "users.db_user"
INDEX_PWD = "users.db_passwd"
INDEX_TABLE = "users.table"
INDEX_USER_COL = "users.user_column"
INDEX_PWD_COL = "users.password_column"

MysqlInfoMap = {INDEX_HOST: '', INDEX_DB: '', INDEX_USER: '', INDEX_PWD: '', INDEX_TABLE: '', INDEX_USER_COL: '',
                INDEX_PWD_COL: ''}

MYSQL_INFO_FILE = "/etc/pam_mysql_arcp.conf"
# ------sqlite info-------------
dbFilename = 'modify_user_time.db'
tablename = 'usertime'
userColumn = 'username'
timeColumn = 'lasttime'
# ---------------------
HOME_DIR_BASE = "/home_i1/"
SOFT_LIMIT_DAY = 90
SOFT_LIMIT_EPOCH = SOFT_LIMIT_DAY*86400
LOGGING_FILE = 'del_inactive_user.log'
logging.basicConfig(filename=LOGGING_FILE, level=logging.WARNING, format='%(asctime)s [%(levelname)s] %(message)s')


def main():
    if os.path.isfile(MYSQL_INFO_FILE):
        parse_mysql_info(MYSQL_INFO_FILE)
        db = connect_mysql()
        if db is not None:
            cursor = db.cursor()
            nn = get_immediate_subdirectories(HOME_DIR_BASE)
            for x in nn:
                inactive = inactive_uid(cursor,x)
                if inactive is not None and inactive is not 0:
                    is_user_exist = False
                else:
                    is_user_exist = True
                # mv /home_i1/<username> to /home_i1/.<username>
                if check_expired(x,is_user_exist):
                    user_home = HOME_DIR_BASE+x
                    user_home_hide = HOME_DIR_BASE+"."+x
                    shutil.move(user_home,user_home_hide)
                    logging.info("delete "+user_home)

            db.close()
    else: logging.error("%s doesn't exist" % MYSQL_INFO_FILE)


def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]


def parse_mysql_info(infile):
    with open(infile) as f:
        for line in f:
            l = line.split('=')
            if len(l) == 2:
                MysqlInfoMap[l[0]] =l[1].rstrip()


def connect_mysql():
    try:
        return MySQLdb.connect(MysqlInfoMap[INDEX_HOST],MysqlInfoMap[INDEX_USER],MysqlInfoMap[INDEX_PWD],MysqlInfoMap[INDEX_DB])
    except:
        logging.error("mysql connection failed")


def inactive_uid(cursor,username):
    sql = "SELECT %s,%s FROM %s WHERE %s='%s' " %(MysqlInfoMap[INDEX_USER_COL],MysqlInfoMap[INDEX_PWD_COL],MysqlInfoMap[INDEX_TABLE],MysqlInfoMap[INDEX_USER_COL],username)
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(results) > 0:
            for row in results:
                name = row[0]
                pwd = row[1]
                logging.debug("name=%s,pwd=%s" % (name,pwd ))
        else:
            owner_uid = stat(HOME_DIR_BASE+username).st_uid
            logging.debug("uid = " + owner_uid)
            return owner_uid
    except:
        logging.error("unable to fetch data")


def check_expired(username, user_exist):
    conn = sqlite3.connect(dbFilename)
    c = conn.cursor()

    c.execute('create table if not exists %s (%s,%s)' % (tablename, userColumn, timeColumn) )
    if user_exist:
        # if exist sqlite, delete it
        c.execute("delete from  %s where %s=?" % (tablename,userColumn) ,(username,))
    else:
        # if not exist sqlite, create and insert current time
        current_time = time.time()
        c.execute("insert into %s (username,lasttime) select '%s',%d where not exists"
                  "(select * from %s where %s=?)" % (tablename, username, current_time, tablename, userColumn)
                  , (username,))
        record_time = sys.maxint
        cursor = conn.execute("select %s from %s where username = ?" % (timeColumn, tablename), (username,))
        for row in cursor:
            record_time = row[0]

        if (current_time-record_time) >= SOFT_LIMIT_EPOCH:
            # return true if expired
            return True

    conn.commit()
    c.close()
    conn.close()


if __name__ == "__main__":
    main()
