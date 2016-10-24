#! /bin/bash

curl -i -X PUT "http://hcgwc112:14000/webhdfs/v1/user/$USER/?user.name=hdfs&op=MKDIRS&permission=700" > /dev/null 2>&1
curl -i -X PUT "http://hcgwc112:14000/webhdfs/v1/user/$USER/?user.name=hdfs&op=SETOWNER&owner=$USER&group=$USER" > /dev/null 2>&1

curl -i -X PUT "http://hcgwc112:14000/webhdfs/v1/user/hive/warehouse/$USER.db/?user.name=hdfs&op=MKDIRS&permission=700" > /dev/null 2>&1
curl -i -X PUT "http://hcgwc112:14000/webhdfs/v1/user/hive/warehouse/$USER.db/?user.name=hdfs&op=SETOWNER&owner=$USER" > /dev/null 2>&1

