#!/usr/bin/python
import MySQLdb
import sys
import os.path
import time
from os import stat

INDEX_HOST = "users.host"
INDEX_DB = "users.database"
INDEX_USER = "users.db_user"
INDEX_PWD = "users.db_passwd"
INDEX_TABLE = "users.table"
INDEX_USER_COL = "users.user_column"
INDEX_PWD_COL = "users.password_column"

dict = {INDEX_HOST: '',INDEX_DB:'',INDEX_USER:'',INDEX_PWD:'',INDEX_TABLE:'',INDEX_USER_COL:'',INDEX_PWD_COL:''};

MYSQL_INFO_FILE = "/etc/pam_mysql_arcp.conf"
HOME_DIR_BASE = "/home_i1/"
SOFT_LIMIT_DAY = 14


def main():
	if os.path.isfile(MYSQL_INFO_FILE):
		parse_mysql_info(MYSQL_INFO_FILE)
		db = connect_mysql()
		if db is not None:
			cursor = db.cursor()
			nn = get_immediate_subdirectories(HOME_DIR_BASE)
			for x in nn:
				inactive = inactive_uid(cursor,x)
	        		if inactive is not None and inactive is not 0 and is_expire(HOME_DIR_BASE+x):
					print "delete "+x
				else: print x+" do nothing"
			db.close()
	else :print "%s doesn't exist" % (MYSQL_INFO_FILE)


def inactive_uid(cursor,username):
    sql = "SELECT %s,%s FROM %s WHERE %s='%s' " %(dict[INDEX_USER_COL],dict[INDEX_PWD_COL],dict[INDEX_TABLE],dict[INDEX_USER_COL],username)
    try:
		cursor.execute(sql)
		results = cursor.fetchall()
		if len(results)>0:
			for row in results:
		      		name = row[0]
				pwd=row[1]
				#print "name=%s,pwd=%s" % (name,pwd )
	 	else:
			owner_uid = stat(HOME_DIR_BASE+username).st_uid
			#print owner_uid
			return owner_uid
    except:
		print "Error: unable to fecth data"


def get_immediate_subdirectories(a_dir):
    return [ name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

def is_expire(user_home):
    mtime = stat(user_home).st_mtime
    now_ts = time.time()
    if now_ts > SOFT_LIMIT_DAY * 24 * 60 * 60 + mtime:
        return True
    else:
        return False

def parse_mysql_info(infile):
	with open(infile) as f:
		for line in f:
			l = line.split('=')
			if(len(l)==2):
				dict[l[0]] =l[1].rstrip()

def connect_mysql():
	try:
		return MySQLdb.connect(dict[INDEX_HOST],dict[INDEX_USER],dict[INDEX_PWD],dict[INDEX_DB])
	except:
		print "Error: mysql connection failed"



if __name__ == "__main__":
    main()
