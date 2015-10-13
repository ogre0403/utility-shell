#!/usr/bin/python
import MySQLdb
import sys
import os
import shutil
import time
from os import stat
from pwd import getpwuid


HOME_DIR_BASE = "/home_i1/"
SOFT_LIMIT_DAY = 14

def main():
    nn = get_immediate_subdirectories(HOME_DIR_BASE)
    for x in nn:
        if inactive_uid(x) is not None and is_expire(x):
            shutil.rmtree(x)


def inactive_uid(user_home):
    owner_uid = stat(user_home).st_uid
    try:
        getpwuid(owner_uid).pw_name
    except KeyError:
        return owner_uid

def get_immediate_subdirectories(a_dir):
    return [HOME_DIR_BASE + name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

def is_expire(user_home):
    mtime = stat(user_home).st_mtime
    now_ts = time.time()
    if now_ts > SOFT_LIMIT_DAY * 24 * 60 * 60 + mtime:
        return True
    else:
        return False

if __name__ == "__main__":
    main()
