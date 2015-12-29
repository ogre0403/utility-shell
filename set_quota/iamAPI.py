#!/usr/bin/env python
# coding=utf-8

import json, requests

requests.packages.urllib3.disable_warnings()

# Setting value
w_setting = {}
w_setting['APP_PRIVATE_ID'] = 'c183df7a1f37423b9e4bcd091153b808'                    # Hadoop 帳號
w_setting['APP_PRIVATE_PASSWD'] = 'c629f93af7524a95a1a46b65f39b42f7'                # Hadoop 密碼
w_setting['APP_UNIX_ACCOUNT_GROUP_UUID'] = "40a7b7b2-5b7f-4d69-b59e-88e79d40d50a"   # Unix 認證主機群組的唯一識別碼
w_setting['APP_UNIX_ACCOUNT_GROUP_UUID_LIST'] = ["40a7b7b2-5b7f-4d69-b59e-88e79d40d50a"]
w_gid_list = []

# WS-Z01-A0-01 : 進行應用系統身分認證
# https://iam-api.nchc.org.tw/app/request_basic_authentication/
def request_basic_authentication(w_id, w_passwd):
    # 取得應用程式 SSO 權杖
    url = "https://iam-api.nchc.org.tw/app/request_basic_authentication/"
    params = dict(
        APP_PRIVATE_ID = w_id,
        APP_PRIVATE_PASSWD = w_passwd
    )
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data

# WS-Z01U-D-A06 : 列出 Unix 認證主機群組清單
# https://iam-api.nchc.org.tw/unix_account_group/list_unix_account_groups/
def unix_account_group_list_unix_account_groups(auth_data):
    url = "https://iam-api.nchc.org.tw/unix_account_group/list_unix_account_groups/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN=auth_data['PRIVILEGED_APP_SSO_TOKEN']
    )
    response = requests.get(url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data

# WS-Z01U-D-B01 : 新增單一 Unix 群組
# https://iam-api.nchc.org.tw/unix_group/add_unix_group/
def unix_group_add_unix_group(auth_data):
    url = "https://iam-api.nchc.org.tw/unix_group/add_unix_group/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        APP_UNIX_ACCOUNT_GROUP_UUID = auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'],
        APP_UNIX_GROUP_UUID = auth_data['APP_UNIX_GROUP_UUID']
    )
    print params
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data

# WS-Z01U-D-B03 : 異動單一Unix 群組屬性
# https://iam-api.nchc.org.tw/unix_group/update_unix_group/
def unix_group_update_unix_group(auth_data):
    url = "https://iam-api.nchc.org.tw/unix_group/update_unix_group/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        APP_UNIX_ACCOUNT_GROUP_UUID = auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'],
        APP_UNIX_GROUP_UUID = auth_data['APP_UNIX_GROUP_UUID'],
        APP_UNIX_GROUP_BASIC_PROFILE = auth_data['APP_UNIX_GROUP_BASIC_PROFILE']
    )
    print params
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data

# WS-Z01U-D-B05 : 取回多重Unix 群組屬性
# https://iam-api.nchc.org.tw/unix_group/get_unix_groups/
def unix_group_get_unix_groups(auth_data):
    url = "https://iam-api.nchc.org.tw/unix_group/get_unix_groups/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        APP_UNIX_ACCOUNT_GROUP_UUID = auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'],
        APP_UNIX_GROUP_UUID_LIST = auth_data['APP_UNIX_GROUP_UUID_LIST']
    )
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data

# WS-Z01U-D-B06 : 取回 Unix群組清單
# https://iam-api.nchc.org.tw/unix_group/list_unix_account_groups/
def unix_group_list_unix_account_groups(auth_data):
    url = "https://iam-api.nchc.org.tw/unix_group/list_unix_account_groups/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        APP_UNIX_ACCOUNT_GROUP_UUID_LIST = auth_data['APP_UNIX_ACCOUNT_GROUP_UUID_LIST']
    )
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data

# WS-Z01U-D-C01：新增單一 Unix 人員
# https://iam-api.nchc.org.tw/unix_user/add_unix_user/
def unix_user_add_unix_user(auth_data):
    url = "https://iam-api.nchc.org.tw/unix_user/add_unix_user/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        APP_UNIX_ACCOUNT_GROUP_UUID = auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'],
        APP_UNIX_USER_UUID = auth_data['APP_UNIX_USER_UUID'],
    )
    print params
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data

# WS-Z01U-D-C03：異動單一Unix 人員屬性
# https://iam-api.nchc.org.tw/unix_user/update_unix_user/
def unix_user_update_unix_user(auth_data):
    url = "https://iam-api.nchc.org.tw/unix_user/update_unix_user/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        APP_UNIX_ACCOUNT_GROUP_UUID = auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'],
        APP_UNIX_USER_UUID = auth_data['APP_UNIX_USER_UUID'],
        APP_UNIX_USER_BASIC_PROFILE = auth_data['APP_UNIX_USER_BASIC_PROFILE'],
        APP_UNIX_GROUP_BELONG_TO = auth_data['APP_UNIX_GROUP_BELONG_TO'],
        APP_UNIX_USER_CREDENTIAL_PROFILE = auth_data['APP_UNIX_USER_CREDENTIAL_PROFILE']
    )
    print params
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data

# WS-Z01U-D-C04：取回單一Unix 人員屬性
# https://iam-api.nchc.org.tw/unix_user/get_unix_user/
def unix_user_get_unix_user(auth_data):
    tmp = dict()
    url = "https://iam-api.nchc.org.tw/unix_user/get_unix_user/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        APP_UNIX_ACCOUNT_GROUP_UUID = auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'], 
        APP_UNIX_USER_UUID = auth_data['APP_UNIX_USER_UUID']
    )
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)

    return data

# WS-Z01U-D-C05：取回多重Unix 人員屬性
# https://iam-api.nchc.org.tw/unix_user/get_unix_users/
def unix_user_get_unix_users(auth_data):
    tmp = dict()
    url = "https://iam-api.nchc.org.tw/unix_user/get_unix_users/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        APP_UNIX_ACCOUNT_GROUP_UUID = auth_data['APP_UNIX_ACCOUNT_GROUP_UUID'], 
        APP_UNIX_USER_UUID_LIST = auth_data['APP_UNIX_USER_UUID_LIST']
    )
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)

    return data

# WS-Z01U-D-C06 : 取回 Unix人員清單
# https://iam-api.nchc.org.tw/unix_user/list_unix_account_users/
def list_unix_account_users(auth_data):
    url = "https://iam-api.nchc.org.tw/unix_user/list_unix_account_users/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        APP_UNIX_ACCOUNT_GROUP_UUID_LIST = ["40a7b7b2-5b7f-4d69-b59e-88e79d40d50a"] # Hadoop group uuid
    )
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data


# WS-Z05-03a : 查詢交換資訊-未提交密碼
# https://iam-api.nchc.org.tw/info_exchange/query_info/
def query_info(auth_data, key1, key2, key3):
    url = "https://iam-api.nchc.org.tw/info_exchange/query_info/"
    params = dict(
        PRIVILEGED_APP_SSO_TOKEN = auth_data['PRIVILEGED_APP_SSO_TOKEN'],
        PUBLISH_INFO_KEY1 = key1,
        PUBLISH_INFO_KEY2 = key2,
        PUBLISH_INFO_KEY3 = key3
    )
    response = requests.get( url = url, data = json.dumps(params), verify = False)
    data = json.loads(response.text)
    return data
