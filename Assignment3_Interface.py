#!/usr/bin/python2.7
#
# Assignment3 Interface
#
import threading
import psycopg2
import os
import sys

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################


def rangePartition(ratingstablename, numberofpartitions, openconnection, SortingColumnName, min_value, max_value, name):
    #name = "RangeRatingsPart"
    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" %ratingstablename)
        if not bool(cursor.rowcount):
            print "Please Load Ratings Table first!!!"
            return
        cursor.execute("CREATE TABLE IF NOT EXISTS RangeRatingsMetadata(PartitionNum INT, MinRating REAL, MaxRating REAL)")
        MinRating = min_value
        MaxRating = max_value
        step = (MaxRating-MinRating)/(float)(numberofpartitions)    
        i = 0;
        while i < numberofpartitions:
            newTableName = name + `i`
            cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating REAL)" %(newTableName))
            i+=1;

        i = 0;
        while MinRating < MaxRating:
            lowerLimit = MinRating
            upperLimit = MinRating + step
            if lowerLimit < 0:
                lowerLimit = 0.0

            if lowerLimit == MinRating:
                cursor.execute("SELECT * FROM %s WHERE %s >= %f AND %s <= %f" %(ratingstablename,SortingColumnName,lowerLimit,SortingColumnName,upperLimit))
                rows = cursor.fetchall()
                newTableName = name + `i`
                for row in rows:
                    cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" %(newTableName, row[0], row[1], row[2]))

            if lowerLimit != MinRating:
                cursor.execute("SELECT * FROM %s WHERE %s > %f AND %s <= %f" %(ratingstablename,SortingColumnName,lowerLimit,SortingColumnName,upperLimit))
                rows = cursor.fetchall()
                newTableName = name + `i`
                for row in rows:
                    cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" %(newTableName, row[0], row[1], row[2]))
            cursor.execute("INSERT INTO RangeRatingsMetadata (PartitionNum, MinRating, MaxRating) VALUES(%d, %f, %f)" %(i,lowerLimit, upperLimit))
            MinRating = upperLimit
            i+=1;

        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)


def rangePartitionMovie(ratingstablename, numberofpartitions, openconnection, SortingColumnName, min_value, max_value, name):
    #name = "RangeRatingsPart"
    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" %ratingstablename)
        if not bool(cursor.rowcount):
            print "Please Load Movies Table first!!!"
            return
        cursor.execute("CREATE TABLE IF NOT EXISTS RangeMoviesMetadata(PartitionNum INT, MinRating REAL, MaxRating REAL)")
        MinRating = min_value
        MaxRating = max_value
        step = (MaxRating-MinRating)/(float)(numberofpartitions)    
        i = 0;
        while i < numberofpartitions:
            newTableName = name + `i`
            cursor.execute("CREATE TABLE IF NOT EXISTS %s(MovieId1 INT,  Title VARCHAR(300),  Genre VARCHAR(300))" %(newTableName))
            i+=1;

        i = 0;
        while MinRating < MaxRating:
            lowerLimit = MinRating
            upperLimit = MinRating + step
            if lowerLimit < 0:
                lowerLimit = 0.0

            if lowerLimit == MinRating:
                cursor.execute("SELECT * FROM %s WHERE %s >= %f AND %s <= %f" %(ratingstablename,SortingColumnName,lowerLimit,SortingColumnName,upperLimit))
                rows = cursor.fetchall()
                newTableName = name + `i`
                for row in rows:
                	cursor.execute("""INSERT INTO """ +newTableName+ """ (MovieId1, Title, Genre) VALUES(%s, %s, %s)""",(row[0], row[1], row[2]))

            if lowerLimit != MinRating:
                cursor.execute("SELECT * FROM %s WHERE %s > %f AND %s <= %f" %(ratingstablename,SortingColumnName,lowerLimit,SortingColumnName,upperLimit))
                rows = cursor.fetchall()
                newTableName = name + `i`
                for row in rows:
                    cursor.execute("INSERT INTO """ +newTableName+ """ (MovieId1, Title, Genre) VALUES(%s, %s, %s)""",(row[0], row[1], row[2]))
            cursor.execute("INSERT INTO RangeMoviesMetadata (PartitionNum, MinRating, MaxRating) VALUES(%d, %f, %f)" %(i,lowerLimit, upperLimit))
            MinRating = upperLimit
            i+=1;

        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)

    

class myThread (threading.Thread):
    def __init__(self, threadID, Tablename, Columnname,connection,OutputTable):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.Tablename = Tablename
        self.Columnname = Columnname
        self.connection = connection
        self.OutputTable = OutputTable
    def run(self):
        print "Starting " + str(self.threadID)
        sorting(self.threadID,self.Tablename,self.Columnname,self.connection,self.OutputTable)
        print "Exiting " + str(self.threadID)


def sorting(id,Tablename, Columnname,connection,OutputTable):
    cur = connection.cursor()
    table = "rangetable"+str(id)
    cur.execute("CREATE TABLE IF NOT EXISTS " + table +" AS SELECT * FROM " +Tablename+ " order by " +Columnname)
    connection.commit()
    pass

class myThreadMovie (threading.Thread):
    def __init__(self, threadID, Tablename1,Tablename2,Columnname1,Columnname2,connection,OutputTable):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.Tablename1 = Tablename1
        self.Tablename2 = Tablename2
        self.Columnname1 = Columnname1
        self.Columnname2 = Columnname2
        self.connection = connection
        self.OutputTable = OutputTable
    def run(self):
        print "Starting " + str(self.threadID)
        parjoin(self.threadID,self.Tablename1,self.Tablename2,self.Columnname1,self.Columnname2,self.connection,self.OutputTable)
        print "Exiting " + str(self.threadID)


def parjoin(id,Tablename1,Tablename2, Columnname1,Columnname2,connection,OutputTable):
    cur = connection.cursor()
    table = "jointable"+str(id)
    cur.execute("CREATE TABLE IF NOT EXISTS " + table +" AS SELECT * FROM " +Tablename1+ " inner join " +Tablename2+ " on " +Tablename1+"."+Columnname1+"="+Tablename2+"."+Columnname2)
    connection.commit()
    pass

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    con = openconnection
    cur = openconnection.cursor();

    cur.execute("SELECT min("+SortingColumnName+") from " +InputTable)
    minvaluearr = cur.fetchone()
    minvalue = minvaluearr[0]
    print minvalue

    cur.execute("SELECT max("+SortingColumnName+") from " +InputTable)
    maxvaluearr = cur.fetchone()
    maxvalue = maxvaluearr[0]

    cur.execute("CREATE TABLE "+OutputTable+" (UserID INT,  MovieID INT ,  Rating REAL)")

    print maxvalue
    #cur.execute("CREATE TABLE IF NOT EXISTS chk (Partition INTEGER,TableName Varchar(50))")


    rangePartition(InputTable,5,openconnection,SortingColumnName,minvalue,maxvalue,"RangeRatingsPart")
    #chk = openconnection


    connection = [getOpenConnection(dbname='ddsassignment3'),getOpenConnection(dbname='ddsassignment3'),getOpenConnection(dbname='ddsassignment3'),getOpenConnection(dbname='ddsassignment3'),getOpenConnection(dbname='ddsassignment3')]
    #cur1 = chk.cursor()

    
    
 
    thread1 = myThread(0, "RangeRatingsPart0", SortingColumnName,connection[0],OutputTable)
    thread2 = myThread(1, "RangeRatingsPart1", SortingColumnName,connection[1],OutputTable)
    thread3 = myThread(2, "RangeRatingsPart2", SortingColumnName,connection[2],OutputTable)
    thread4 = myThread(3, "RangeRatingsPart3", SortingColumnName,connection[3],OutputTable)
    thread5 = myThread(4, "RangeRatingsPart4", SortingColumnName,connection[4],OutputTable)

	# Start new Threads
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    print "Exiting Main Thread"

    thread1.join()
    thread2.join();
    thread3.join();
    thread4.join();
    thread5.join();

    conn = getOpenConnection(dbname='ddsassignment3')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur1 = conn.cursor()
    

    for i in range(0,5):
        cur1.execute("INSERT INTO " +OutputTable+ " SELECT * FROM rangetable"+str(i))

    query = "Select * from " +OutputTable
    outputquery = "COPY ({0}) TO STDOUT WITH DELIMITER AS ','".format(query)
    #f = open('outputfile.dat', 'w+')
    
    try:
    	os.remove('output.txt')
    except OSError:
    	pass

    with open('output.txt','a+') as f:
        cur.copy_expert(outputquery,f)


    pass #Remove this once you are done with implementation

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    con = openconnection
    cur = openconnection.cursor();

    cur.execute("SELECT min("+Table2JoinColumn+") from " +InputTable2)
    minvaluearr = cur.fetchone()
    minvalue = minvaluearr[0]
    print minvalue

    cur.execute("SELECT max("+Table2JoinColumn+") from " +InputTable2)
    maxvaluearr = cur.fetchone()
    maxvalue = maxvaluearr[0]

    cur.execute("CREATE TABLE "+OutputTable+" (UserID INT,  MovieID INT ,  Rating REAL, MovieId1 INT,  Title VARCHAR(100),  Genre VARCHAR(100))")

    print maxvalue
    #print InputTable2
    #print Table2JoinColumn

    rangePartition(InputTable1,5,openconnection,Table1JoinColumn,minvalue,maxvalue,"RatingsPartition")
    rangePartitionMovie(InputTable2,5,openconnection,Table2JoinColumn,minvalue,maxvalue,"MoviePartition")


    connection = [getOpenConnection(dbname='ddsassignment3'),getOpenConnection(dbname='ddsassignment3'),getOpenConnection(dbname='ddsassignment3'),getOpenConnection(dbname='ddsassignment3'),getOpenConnection(dbname='ddsassignment3')]
    #cur1 = chk.cursor()

    
    
 
    thread1 = myThreadMovie(0, "RatingsPartition0","MoviePartition0", Table1JoinColumn,Table2JoinColumn,connection[0],OutputTable)
    thread2 = myThreadMovie(1, "RatingsPartition1","MoviePartition1", Table1JoinColumn,Table2JoinColumn,connection[1],OutputTable)
    thread3 = myThreadMovie(2, "RatingsPartition2","MoviePartition2", Table1JoinColumn,Table2JoinColumn,connection[2],OutputTable)
    thread4 = myThreadMovie(3, "RatingsPartition3","MoviePartition3", Table1JoinColumn,Table2JoinColumn,connection[3],OutputTable)
    thread5 = myThreadMovie(4, "RatingsPartition4","MoviePartition4", Table1JoinColumn,Table2JoinColumn,connection[4],OutputTable)

	# Start new Threads
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    print "Exiting Main Thread"

    thread1.join()
    thread2.join();
    thread3.join();
    thread4.join();
    thread5.join();
    conn = getOpenConnection(dbname='ddsassignment3')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur1 = conn.cursor()
    

    for i in range(0,5):
        cur1.execute("INSERT INTO " +OutputTable+ " SELECT * FROM jointable"+str(i))

    query = "Select * from " +OutputTable
    outputquery = "COPY ({0}) TO STDOUT WITH DELIMITER AS ','".format(query)
    #f = open('outputfile.dat', 'w+')
    try:
    	os.remove('outputjoin.txt')
    except OSError:
    	pass

    with open('outputjoin.txt','a+') as f:
        cur.copy_expert(outputquery,f)


    pass # Remove this once you are done with implementation


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
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
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
