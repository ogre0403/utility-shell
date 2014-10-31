#!/bin/bash

[ -r "hostnames.txt" ] || {
    echo "hostnames.txt does not exist"
    exit 1
}

krb_realm=NCHC.TC
namenode_primary=hdadm
datanode_primary=hdadm
secondarynn_primary=hdadm
journalnode_primary=hdadm
resourcemanager_primary=hdadm
nodemanager_primary=hdadm
jobhistory_primary=hdadm

KEYTAB_PATH_IN_EACH=/tmp/keytab
HDADM_IN_EACH=hdadm

for NAME in $(cat hostnames.txt); do
    host=${NAME,,}
    install -d ${host}
    kadmin.local -q "addprinc -randkey ${namenode_primary}/${host}@${krb_realm}"; 
    kadmin.local -q "addprinc -randkey ${secondarynn_primary}/${host}@${krb_realm}"; 
    kadmin.local -q "addprinc -randkey ${datanode_primary}/${host}@${krb_realm}"; 
    kadmin.local -q "addprinc -randkey ${journalnode_primary}/${host}@${krb_realm}"; 
    kadmin.local -q "addprinc -randkey ${resourcemanager_primary}/${host}@${krb_realm}"; 
    kadmin.local -q "addprinc -randkey ${nodemanager_primary}/${host}@${krb_realm}"; 
    kadmin.local -q "addprinc -randkey ${jobhistory_primary}/${host}@${krb_realm}"; 
    kadmin.local -q "addprinc -randkey HTTP/${host}@${krb_realm}"; 
    kadmin.local -q "addprinc -randkey host/${host}@${krb_realm}"; 
    kadmin.local -q "ktadd -k ${host}/hdadm.keytab -norandkey \
                    ${namenode_primary}/${host}@${krb_realm} \
                    ${secondarynn_primary}/${host}@${krb_realm} \
                    ${datanode_primary}/${host}@${krb_realm} \
                    ${journalnode_primary}/${host}@${krb_realm} \
                    ${resourcemanager_primary}/${host}@${krb_realm} \
                    ${nodemanager_primary}/${host}@${krb_realm} \
                    ${jobhistory_primary}/${host}@${krb_realm} \
                    host/${host}@${krb_realm} \
                    HTTP/${host}@${krb_realm}";\
    chown $HDADM_IN_EACH:$HDADM_IN_EACH ${host}/hdadm.keytab;
    scp ${host}/hdadm.keytab hdadm@${NAME}:${KEYTAB_PATH_IN_EACH}/hdadm.keytab;
    rm -rf ${host};
    #kadmin.local -q "ktadd -k ${name}/hdadm.keytab -norandkey hdadm/${name}@${krb_realm} host/${name}@${krb_realm} HTTP/${name}@${krb_realm}"
done
