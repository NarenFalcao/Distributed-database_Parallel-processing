#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import sys
import csv
import math

DATABASE_NAME = 'dds_assgn1'

range_prefix = 'range_part'
robin_prefix = 'rrobin_part'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    #print ratingstablename
    #print ratingsfilepath
    f=open(ratingsfilepath,'rb') # opens file for reading
    reader = csv.reader(f)
    cur = openconnection.cursor()
    #print cur
    ratingstemp = ratingstablename+"temp"
    cur.execute("CREATE TABLE IF NOT EXISTS " + ratingstemp + "(UserId INTEGER, temp1 char, MovieID INTEGER, temp2 char, Rating NUMERIC, temp3 char, temp4 NUMERIC)")
    cur.execute("CREATE TABLE IF NOT EXISTS " + ratingstablename +"(UserId INTEGER, MovieID INTEGER, Rating NUMERIC)")  
    cur.execute("DELETE FROM "+ratingstemp)
    cur.execute("DELETE FROM "+ratingstablename)  
    
    copy_sql = "COPY " +ratingstemp+ " FROM stdin DELIMITER as ':'"
    with open(ratingsfilepath, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)  
        
    #print ratingsfilepath
    cur.execute("ALTER TABLE " +ratingstemp+" DROP temp1, DROP temp2, DROP temp3, DROP temp4")
    cur.execute("INSERT INTO " +ratingstablename+" SELECT * FROM " +ratingstemp)
    cur.execute("DROP TABLE "+ratingstemp)
    openconnection.commit()
    pass


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    start = 0
    diff = 5.0/numberofpartitions
    end = 5.0/numberofpartitions
    cur = openconnection.cursor()
    diff = round(diff,2)
    end = round(end,2)
    global range_prefix
    range_prefix_tname = range_prefix
    cur.execute("CREATE TABLE IF NOT EXISTS meta_rp (Partition INTEGER,TableName Varchar(50))")
    cur.execute("DELETE FROM meta_rp")
    cur.execute("INSERT INTO meta_rp (Partition,TableName) values (%s,%s)",(numberofpartitions,range_prefix_tname))
    #global partition_no_global 
    #partition_no_global = numberofpartitions
    #global tablename_global 
    #tablename_global = ratingstablename
    for x in range(0,numberofpartitions):
        tablename = range_prefix_tname+str(x)
        var1 = 200
        
        if x==0:
            #print "last"
            #print start
            #print end
            cur.execute("CREATE TABLE " + tablename +" AS SELECT * FROM " +ratingstablename+ " WHERE Rating>= " +str(start)+ " AND Rating<=" +str(end)  )
        else:
            #print "others"
            #print start
            #print end
            cur.execute("CREATE TABLE " + tablename +" AS SELECT * FROM " +ratingstablename+ " WHERE Rating> " +str(start)+ " AND Rating<=" +str(end)  )  
        start = end
        end = end+diff


    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    global robin_prefix
    robin_prefix_tname = robin_prefix
    cur = openconnection.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS meta_rrp (Partition INTEGER,TableName Varchar(50))")
    cur.execute("DELETE FROM meta_rrp")
    cur.execute("INSERT INTO meta_rrp (Partition,TableName) values (%s,%s)",(numberofpartitions,robin_prefix_tname))
    
    for x in range(0,numberofpartitions):
        tablename = robin_prefix_tname+str(x%numberofpartitions)
        cur.execute("CREATE TABLE " + tablename +" AS SELECT *  from (SELECT " +ratingstablename+ ".*, row_number() OVER() AS rnum from " +ratingstablename + " ) as x WHERE MOD(x.rnum," +str(numberofpartitions)+ ")=" +str((x+1)%numberofpartitions))
        cur.execute("ALTER table "+tablename+ " DROP rnum")

    cur.execute("SELECT COUNT(*) from " +ratingstablename)
    count = cur.fetchone();
    cnt = count[0]
    #print cnt    
    cur.execute("CREATE TABLE IF NOT EXISTS meta_rrp_cnt (Count Integer)")
    cur.execute("DELETE FROM meta_rrp_cnt")
    cur.execute("INSERT INTO meta_rrp_cnt (Count) VALUES (%s)",(cnt,))
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if rating > 5 or rating < 0:
        print "Values are not compatible"
        return
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM meta_rrp LIMIT 1")
    rowval = cur.fetchone();
    partition_no_rr = rowval[0]
    partition_name_rr = rowval[1]

    #print partition_no_rr
    #print partition_name_rr
    
    #cur.execute("INSERT into " +ratingstablename+ "(UserId,MovieID,Rating) VALUES (%s, %s, %s)", (userid,itemid,rating))
    
    cur.execute("SELECT * from meta_rrp_cnt LIMIT 1")
    count = cur.fetchone();
    cnt = count[0]
    #print "insertcount:"
    #print cnt
    cnt  = cnt+1
    cur.execute("UPDATE meta_rrp_cnt SET Count =" +str(cnt))
    
    modval = (cnt-1)%partition_no_rr
    tablename = partition_name_rr+str(modval)
    cur.execute("INSERT INTO " +tablename+ " (UserId, MovieID, Rating) VALUES (%s,%s,%s)",(userid,itemid,rating))

    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if rating > 5 or rating < 0:
        print "Values are not compatible"
        return
    cur = openconnection.cursor()
    #partition_no_global = 3;
    #tablename_global = 'nk2'
    cur.execute("SELECT * FROM meta_rp LIMIT 1")
    rowval = cur.fetchone();
    partition_no_ri = rowval[0]
    partition_name_ri = rowval[1]
    start = 0
    diff = 5.0/partition_no_ri
    end = 5.0/partition_no_ri
    diff = round(diff,2)
    end = round(end,2)
    #print end
    for x in range(0,partition_no_ri):
        if x==0:
            if rating>=start and rating<=end:
                tablename = partition_name_ri+str(x)
                cur.execute("INSERT INTO " +tablename+ " (UserId, MovieID, Rating) VALUES (%s,%s,%s)",(userid,itemid,rating))
        else:
            if rating>start and rating<=end:
                tablename = partition_name_ri+str(x)
                cur.execute("INSERT INTO " +tablename+ " (UserId, MovieID, Rating) VALUES (%s,%s,%s)",(userid,itemid,rating))
    

        
        start = end
        end = end+diff    
    pass


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    #range partition deletion
    cur  = openconnection.cursor()
    cur.execute("SELECT * FROM meta_rp LIMIT 1")
    rowval = cur.fetchone();
    partition_no_ri = rowval[0]
    partition_name_ri = rowval[1]

    for x in range(0,partition_no_ri):
        tablename = partition_name_ri+str(x)
        cur.execute("DROP TABLE IF EXISTS "+tablename )


    #rr parition deletion
    cur.execute("SELECT * FROM meta_rrp LIMIT 1")
    rowval = cur.fetchone();
    partition_no_rr = rowval[0]
    partition_name_rr = rowval[1]
    for x in range(0,partition_no_rr):
        tablename = partition_name_rr+str(x%partition_no_rr)
        cur.execute("DROP TABLE IF EXISTS "+tablename )


    #delete the meta tables
    cur.execute("DROP TABLE IF EXISTS meta_rp")
    cur.execute("DROP TABLE IF EXISTS meta_rrp")
    cur.execute("DROP TABLE IF EXISTS meta_rrp_cnt")
    pass


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            #loadratings('nk4','/home/naren/Desktop/DDS/test_data.dat', con)
            #rangepartition('nk4',3, con)
            #rangeinsert('nk4', 35, 1196, 5, con)
            #roundrobinpartition('nk4',4, con)
            #roundrobininsert('nk4', 33, 1192, 2, con)
            #deletepartitionsandexit(con)
            # ###################################################################################
            # Anything in this area will no/home/naren/Desktop/DDS/Tester/t be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
