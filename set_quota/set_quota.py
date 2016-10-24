#!/usr/bin/python
# coding=utf-8

from iamAPI import *
from datetime import  timedelta
import logging
import subprocess
import os
import datetime


LOGGING_FILE = '/etc/braavos/set_quota/set_quota.log'
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

    key2 = get_key2()
    logger.info("Fetch quota setting with key2:{%s}" %(key2))
    result = query_quota_info(auth_data, key2)
    set_all_user_quota(result)


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
        key2 = yesterday_2100.strftime('%Y%m%d%H%M')
    elif now > today_1200 and now < today_2100:
        key2 = today_1200.strftime('%Y%m%d%H%M')
    elif now > today_1200 and now > today_2100:
        key2 = today_2100.strftime('%Y%m%d%H%M')
    else:
        key2 = yesterday_2100.strftime('%Y%m%d%H%M')
        logger.warn("should not be here")

    return key2


'''
quota_setting is JSON ARRAY with following format:
[{"k00jmy00":{"STATUS":"NEW","LOCALFS_QUOTA":{"filesystem":"/home_i1/","soft":"450","hard":"500"},"HDFS_QUOTA":{"filesystem":"/user/k00jmy00","number":"10000","space":"500"}}}]
'''
def set_all_user_quota(quota_setting):
    if quota_setting is not None:
        for user_setting in quota_setting:
            name = user_setting.keys()[0]
            setting = user_setting[name]
            setQuota(name, setting)
    else:
        logger.info("No NEW or UPDATE quota info")

def setQuota(name, quota_setting):
    logger.info("{%s} {%s} quota" % (name, quota_setting['STATUS']))
    setLocalFSQuota(name, quota_setting['LOCALFS_QUOTA'])
    setHDFSQuota(name, quota_setting['HDFS_QUOTA'])


def setHDFSQuota(name, hdfs_quota_setting):
    fs = hdfs_quota_setting['filesystem']
    number = hdfs_quota_setting['number']
    # default unit is 2^9 (G), space should be multiplied by replication number
    space = str(int(hdfs_quota_setting['space'])*3)+"g"
    logger.info("set %s HDFS quota <dir:%s, number:%s, space:%s>" % (name, fs, number, space))

    if is_windows() != True:
        # create HDFS user home if not exist
        if is_HDFS_dir_exist(fs) == False:
            # sudo -u hdfs hadoop fs -mkdir /user/k00jmy00
            sp = subprocess.Popen(['sudo', '-u', 'hdfs', 'hadoop', 'fs', '-mkdir', fs],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        close_fds=True)
            stderr = sp.communicate()[1]
            if sp.returncode != 0:
                logger.error("set HDFS space quota fail with STDERR %s" % (stderr))

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

'''
return True if dir exist
Usage: hdfs dfs -test -[ezd] URI
Options:
-e check to see if the file exists. Return 0 if true.
-z check to see if the file is zero length. Return 0 if true.
-d check to see if the path is directory. Return 0 if true.
'''
def is_HDFS_dir_exist(dir):
    sp = subprocess.Popen(['hdfs', 'dfs', '-test', '-e', dir],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            close_fds=True)
    sp.communicate()
    if sp.returncode == 0:
        return True
    else:
        return False


def is_windows():
    return os.name == 'nt'


if __name__ == "__main__":
    main()
