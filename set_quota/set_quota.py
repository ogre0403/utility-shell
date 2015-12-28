#!/usr/bin/python
# coding=utf-8

from iamAPI import *
import logging
import subprocess


LOGGING_FILE = 'set_quota.log'
logging.basicConfig(filename=LOGGING_FILE,
                    level=logging.INFO,
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

    logger.info("Get UUID/Name map")
    id_and_name = getUuidNameMap(auth_data)

    logger.info("Get User quota configuration and set")
    for key, val in id_and_name.items():
        content = getUserQuota(auth_data, key, val)
        if content is not None:
            setQuota(content, val)


def basicAuth():
    auth_data = request_basic_authentication(w_setting['APP_PRIVATE_ID'], w_setting['APP_PRIVATE_PASSWD'])
    if auth_data['ERROR_CODE'] == '0':
        logger.debug("PRIVILEGED_APP_SSO_TOKEN: %s" % (auth_data['PRIVILEGED_APP_SSO_TOKEN']))
        auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'] = w_setting['APP_UNIX_ACCOUNT_GROUP_UUID']
    else:
        logger.error("ERROR_CODE: %s" % (auth_data['ERROR_CODE']))
        logger.debug(auth_data)
    return auth_data


def getUuidNameMap(auth_data):
    #initialize uuid/name dictionary
    uuidNameMap={}

    #Get USER UUID LIST
    list_users = list_unix_account_users(auth_data)
    if list_users['ERROR_CODE'] == '0':
        auth_data['APP_UNIX_USER_UUID_LIST'] = list_users['APP_UNIX_USER_RESULT_LIST'][0]['APP_UNIX_USER_UUID_LIST']
        auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'] = list_users['APP_UNIX_USER_RESULT_LIST'][0]['APP_UNIX_ACCOUNT_GROUP_UUID']
        logger.debug("APP_UNIX_USER_UUID_LIST: %s" % (json.dumps(auth_data['APP_UNIX_USER_UUID_LIST'])))
        logger.debug("APP_UNIX_ACCOUNT_GROUP_UUID: %s" % (auth_data['APP_UNIX_ACCOUNT_GROUP_UUID']))
    else:
        logger.error("ERROR_CODE: %s" % (list_users['ERROR_CODE']))
        logger.debug(json.dumps(list_users))

    #get multiple Unix member attribute
    users_args = unix_user_get_unix_users(auth_data)
    if users_args['ERROR_CODE'] == '0':
        logger.debug(json.dumps(users_args))
        for val in users_args['APP_UNIX_USER_RESULT_LIST']:
            uuidNameMap[val['APP_UNIX_USER_UUID']] = val['APP_UNIX_USER_BASIC_PROFILE']['UNIX_USERNAME']
    else:
        logger.error("ERROR_CODE: %s" % (users_args['ERROR_CODE']))
        logger.debug(json.dumps(users_args))
    return uuidNameMap


def getUserQuota(auth_data, uuid, name):
    info_result = query_quota_info(auth_data, uuid)
    if info_result['ERROR_CODE'] == '0':
        return info_result['PUBLISH_INFO_CONTENT']
    elif info_result['ERROR_CODE'] == '4':
        logger.warn("%s's (%s) quota info not publish, use default setting" % (name, uuid))
        default_quota_setting={'LOCALFS_QUOTA':
                                   {"filesystem":"/home_i1",
                                    "soft":"450",
                                    "hard":"500"},
                               'HDFS_QUOTA':
                                   {"filesystem":"/user/"+name,
                                    "number":"10000",
                                    "space":"500"}
                               }
        return default_quota_setting
    else:
        logger.error("ERROR_CODE: %s" % (info_result['ERROR_CODE']))
        logger.info(json.dumps(info_result))


def setQuota(quota_setting, name):
    setLocalFSQuota(quota_setting['LOCALFS_QUOTA'], name)
    setHDFSQuota(quota_setting['HDFS_QUOTA'], name)


def setHDFSQuota(hdfs_quota_setting, name):
    fs = hdfs_quota_setting['filesystem']
    number = hdfs_quota_setting['number']
    # default unit is 2^9 (G), space should be multiplied by replication number
    space = str(int(hdfs_quota_setting['space'])*3)+"g"
    logger.info("set %s HDFS quota <dir:%s, number:%s, space:%s>" % (name, fs, number, space))

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


def setLocalFSQuota(localfs_quota_setting, name):
    fs = localfs_quota_setting['filesystem']
    # default unit is 2^9 (G)
    soft = localfs_quota_setting['soft']+"G"
    hard = localfs_quota_setting['hard']+"G"
    logger.info("set %s local fs quota  <fs:%s, soft:%s, hard:%s>" % (name, fs, soft, hard))
    subprocess.call(['setquota', '-u', name, soft, hard, '0', '0', fs])


def query_quota_info(auth_data, uuid):
     # TODO: change key2 to $APP_UNIX_ACCOUNT_GROUP_UUID:$APP_UNIX_USER_UUID
     #key2 = w_setting['APP_UNIX_ACCOUNT_GROUP_UUID']+":"+uuid
     key2 = uuid
     return query_info(auth_data, "UNIX_USER_QUOTA" , key2, "hadoo.nchc.org.tw")


if __name__ == "__main__":
    main()