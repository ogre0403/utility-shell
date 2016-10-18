#!/bin/bash
# http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_uninstall_cm.html
# https://niksammy.wordpress.com/2014/04/21/uninstalling-cdh5-cluster/

service cloudera-scm-server stop
service cloudera-scm-server-db stop
service cloudera-scm-agent stop
yum remove -y cloudera-manager-server
/usr/sbin/service cloudera-scm-agent hard_stop
yum remove -y 'cloudera-manager-*'

yum remove -y cloudera-manager-server-db-2 postgresql

yum clean 

for u in cloudera-scm flume hadoop hdfs hbase hive httpfs hue impala llama mapred oozie solr spark sqoop sqoop2 yarn zookeeper; do kill $(ps -u $u -o pid=); done

umount cm_processes

rm -Rf /usr/share/cmf /var/lib/cloudera* /var/cache/yum/cloudera* /var/log/cloudera* /var/run/cloudera*

rm /tmp/.scm_prepare_node.lock

rm -Rf /var/lib/flume-ng /var/lib/hadoop* /var/lib/hue /var/lib/navigator /var/lib/oozie /var/lib/solr /var/lib/sqoop* /var/lib/zookeeper

rm -Rf /usr/lib/cmf

rm -rf /tmp/cmflistener-*

rm -rf /tmp/scm_*

rm -Rf /data1/dfs /data1/mapred /data1/yarn 
rm -Rf /data2/dfs /data2/mapred /data2/yarn 
rm -Rf /data3/dfs /data3/mapred /data3/yarn 

rm -Rf /opt/cloudera /etc/hadoop /etc/cloudera-scm* /etc/hive /var/run/hdfs-* /var/lib/hive /var/lib/alternatives/hive-conf /etc/alternatives/hive-conf /var/run/hbase /var/log/impalad /var/run/impala /var/lib/impala /var/run/sqoop2 /var/run/solr /var/run/zookeeper /var/run/hadoop-httpfs /var/lib/alternatives/hadoop-conf /etc/alternatives/hadoop-conf
