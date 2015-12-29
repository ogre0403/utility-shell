#!/usr/bin/python
# coding=utf-8

from iamAPI import *
from datetime import  timedelta
import logging
import subprocess
import os
import datetime


LOGGING_FILE = 'set_quota.log'
logging.basicConfig(filename=LOGGING_FILE,
                    level=logging.DEBUG,
                    format='%(asctime)s [%(levelname)s] %(filename)s_%(lineno)d  : %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s_%(lineno)d : %(message)s')
console.setFormatter(formatter)
logging.getLogger('root').addHandler(console)
logger = logging.getLogger('root')


def main():
    logger.info("Get SSO token")
    auth_data = basicAuth()

    new_key, update_key = get_key2()
    logger.info("Quota key2 <%s, %s>" %(new_key, update_key))

    logger.info("Fetch new user quota setting")
    new_result = query_quota_info(auth_data, new_key)
    set_all_user_quota(new_result)
    logger.info("Fetch update user quota setting")
    update_result = query_quota_info(auth_data, update_key)
    set_all_user_quota(update_result)


def basicAuth():
    auth_data = request_basic_authentication(w_setting['APP_PRIVATE_ID'], w_setting['APP_PRIVATE_PASSWD'])
    if auth_data['ERROR_CODE'] == '0':
        logger.debug("PRIVILEGED_APP_SSO_TOKEN: %s" % (auth_data['PRIVILEGED_APP_SSO_TOKEN']))
        auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'] = w_setting['APP_UNIX_ACCOUNT_GROUP_UUID']
    else:
        logger.error("ERROR_CODE: %s" % (auth_data['ERROR_CODE']))
        logger.debug(auth_data)
    return auth_data



#        yesterday_2100     today_1200     today_2100
#   --+-------------------+-------------+---------------+---
#    0000                1200          2100            2400
#
def get_key2():

    now = datetime.datetime.now()
    today_1200 = datetime.datetime(now.year, now.month, now.day, 12, 0, 0, 0)
    today_2100 = datetime.datetime(now.year, now.month, now.day, 21, 0, 0, 0)
    yesterday_2100 = today_2100 - timedelta(1)

    if now < today_1200 and now < today_2100:
        newkey = yesterday_2100.strftime('%Y%m%d%H%M')+":NEW"
        updatekey = yesterday_2100.strftime('%Y%m%d%H%M')+":UPDATE"
    elif now > today_1200 and now < today_2100:
        newkey = today_1200.strftime('%Y%m%d%H%M')+":NEW"
        updatekey = today_1200.strftime('%Y%m%d%H%M')+":UPDATE"
    elif now > today_1200 and now > today_2100:
        newkey = today_2100.strftime('%Y%m%d%H%M')+":NEW"
        updatekey = today_2100.strftime('%Y%m%d%H%M')+":UPDATE"
    else:
        newkey = yesterday_2100.strftime('%Y%m%d%H%M')+":NEW"
        updatekey = yesterday_2100.strftime('%Y%m%d%H%M')+":UPDATE"
        logger.warn("should not be here")

    return newkey, updatekey


def set_all_user_quota(quota_setting):
    if quota_setting is not None:
        for name, setting in quota_setting.items():
            setQuota(name, setting)
    else:
        logger.info("No NEW or UPDATE quota info")

def setQuota(name, quota_setting):
    setLocalFSQuota(name, quota_setting['LOCALFS_QUOTA'])
    setHDFSQuota(name, quota_setting['HDFS_QUOTA'])


def setHDFSQuota(name, hdfs_quota_setting):
    fs = hdfs_quota_setting['filesystem']
    number = hdfs_quota_setting['number']
    # default unit is 2^9 (G), space should be multiplied by replication number
    space = str(int(hdfs_quota_setting['space'])*3)+"g"
    logger.info("set %s HDFS quota <dir:%s, number:%s, space:%s>" % (name, fs, number, space))

    if is_windows() != True:
        # sudo -u hdfs hdfs dfsadmin -setQuota 10000 /user/k00jmy00
        sp = subprocess.Popen(['sudo', '-u', 'hdfs', 'hdfs', 'dfsadmin', '-setQuota', number, fs],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              close_fds=True)
        stderr = sp.communicate()[1]
        if sp.returncode != 0:
            logger.error("set HDFS quota fail with STDERR %s" % (stderr))

        # sudo -u hdfs hadoop dfsadmin -setSpaceQuota 1500g /user/k00jmy00
        sp = subprocess.Popen(['sudo', '-u', 'hdfs', 'hdfs', 'dfsadmin', '-setSpaceQuota', space, fs],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        close_fds=True)
        stderr = sp.communicate()[1]
        if sp.returncode != 0:
            logger.error("set HDFS space quota fail with STDERR %s" % (stderr))


def setLocalFSQuota(name, localfs_quota_setting):
    fs = localfs_quota_setting['filesystem']
    # default unit is 2^9 (G)
    soft = localfs_quota_setting['soft']+"G"
    hard = localfs_quota_setting['hard']+"G"
    logger.info("set %s local fs quota  <fs:%s, soft:%s, hard:%s>" % (name, fs, soft, hard))
    if is_windows() != True:
        subprocess.call(['setquota', '-u', name, soft, hard, '0', '0', fs])


def query_quota_info(auth_data, key2):
    quota_result = query_info(auth_data, "UNIX_USER_QUOTA" , key2, "hadoop.nchc.org.tw")
    if quota_result['ERROR_CODE'] == '0':
        return quota_result['PUBLISH_INFO_CONTENT']
    else:
        logger.error("ERROR_CODE: %s" % (quota_result['ERROR_CODE']))
        logger.debug(json.dumps(quota_result))
        return None


def is_windows():
    return os.name == 'nt'


if __name__ == "__main__":
    main()