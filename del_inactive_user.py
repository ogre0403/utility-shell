#!/usr/bin/python
import MySQLdb
import sys
import os.path
from os import stat
import time
import sqlite3
#------mysql info------
INDEX_HOST = "users.host"
INDEX_DB = "users.database"
INDEX_USER = "users.db_user"
INDEX_PWD = "users.db_passwd"
INDEX_TABLE = "users.table"
INDEX_USER_COL = "users.user_column"
INDEX_PWD_COL = "users.password_column"

dict = {INDEX_HOST: '',INDEX_DB:'',INDEX_USER:'',INDEX_PWD:'',INDEX_TABLE:'',INDEX_USER_COL:'',INDEX_PWD_COL:''};

MYSQL_INFO_FILE = "/etc/pam_mysql_arcp.conf"
#------sqlite info-------------
dbFilename = 'modify_user_time.db'
tablename = 'usertime'
userColumn = 'username'
timeColumn = 'lasttime'
#---------------------
HOME_DIR_BASE = "/home_i1/"
SOFT_LIMIT_DAY = 90
SOFT_LIMIT_EPOCH = SOFT_LIMIT_DAY*86400

#connect to database 
def main():
	
	if os.path.isfile(MYSQL_INFO_FILE):
		parse_mysql_info(MYSQL_INFO_FILE)
		db = connect_mysql() 
		if db is not None: 
			cursor = db.cursor()
			nn = get_immediate_subdirectories(HOME_DIR_BASE)
			for x in nn:
				inactive = inactive_uid(cursor,x)
	        		if inactive is not None and inactive is not 0:	isUserExist = False
				else: isUserExist =True 
				if check_expired(x,isUserExist):print "delete "+x

			db.close()
	else :print "%s doesn't exist" % (MYSQL_INFO_FILE)

def get_immediate_subdirectories(a_dir):
    return [ name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]


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

def check_expired(username,userExist):
#	if os.path.isfile(dbFilename):

	conn = sqlite3.connect(dbFilename)
	c = conn.cursor()

	c.execute('create table if not exists %s (%s,%s)' % (tablename, userColumn, timeColumn) )
	if userExist:
	#if exist sqlite, delete it
		c.execute("delete from  %s where %s=?" % (tablename,userColumn) ,(username,))
	else:
	# if not exist sqlite, create and insert current time
		current_time = time.time()
		c.execute("insert into usertime (username,lasttime) select '%s',%d where not exists(select * from %s where %s=?)" % (username,current_time,tablename,userColumn), (username,))
		record_time = sys.maxint
		cursor = conn.execute("select lasttime from usertime where username = ?" ,(username,))
		for row in cursor:
			record_time = row[0]
		#return true if expired
		if (current_time-record_time)>=SOFT_LIMIT_EPOCH:
			return True

	conn.commit()
	c.close()
	conn.close()


if __name__ == "__main__":
    main()
