#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import traceback
import thread
import threading

def threadSortWork(PartitionTable, OutputTable, openConnection, SortingColumn):

    cur = openConnection.cursor()

    drop_stmt_output_table = 'DROP TABLE IF EXISTS ' + OutputTable
    cur.execute(drop_stmt_output_table)

    create_stmt = 'create table '+ OutputTable + ' as select * from ' + PartitionTable + ' Order by ' \
                  + SortingColumn

    cur.execute(create_stmt)

    drop_stmt_partition_table = 'DROP TABLE IF EXISTS ' + PartitionTable
    cur.execute(drop_stmt_partition_table)

    openConnection.commit()

def threadJoinWork(Table1, Table2, JoinCol1, JoinCol2, OutputTable, openConnection):

    cur = openConnection.cursor()

    drop_stmt_output_table = 'DROP TABLE IF EXISTS ' + OutputTable
    cur.execute(drop_stmt_output_table)

    create_stmt = 'create table ' + OutputTable + ' as select * from ' + \
                  Table1 + ' JOIN ' + Table2 + ' ON ' + Table1 + '.' + JoinCol1 + '=' + Table2 + '.' + JoinCol2

    cur.execute(create_stmt)

    drop_stmt_join_tables = 'DROP TABLE IF EXISTS ' + Table1 +', ' + Table2
    cur.execute(drop_stmt_join_tables)

    openConnection.commit()


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    cur = openconnection.cursor()
    rangepartition(InputTable,5,openconnection,SortingColumnName,InputTable)

    thread_1 = threading.Thread(target=threadSortWork, args=('range_part_' + InputTable + '0','partition_sort0',
                                                         openconnection,SortingColumnName))
    thread_2 = threading.Thread(target=threadSortWork, args=('range_part_' + InputTable + '1','partition_sort1',
                                                         openconnection,SortingColumnName))
    thread_3 = threading.Thread(target=threadSortWork, args=('range_part_' + InputTable + '2','partition_sort2',
                                                         openconnection,SortingColumnName))
    thread_4 = threading.Thread(target=threadSortWork, args=('range_part_' + InputTable + '3','partition_sort3',
                                                         openconnection,SortingColumnName))
    thread_5 = threading.Thread(target=threadSortWork, args=('range_part_' + InputTable + '4','partition_sort4',
                                                         openconnection,SortingColumnName))

    thread_1.start()
    thread_2.start()
    thread_3.start()
    thread_4.start()
    thread_5.start()

    thread_1.join()
    thread_2.join()
    thread_3.join()
    thread_4.join()
    thread_5.join()

    drop_stmt = 'DROP TABLE IF EXISTS ' + OutputTable
    cur.execute(drop_stmt)

    create_stmt = 'create table ' + OutputTable + ' as select * from ' + InputTable + ' where 1=0'
    cur.execute(create_stmt)

    for i in (range(0,5)):
        insert_stmt = 'insert into ' + OutputTable + ' (select * from ' + 'partition_sort' + str(i) + ')'
        cur.execute(insert_stmt)

    openconnection.commit()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    cur = openconnection.cursor()

    rangepartition(InputTable1,5,openconnection,Table1JoinColumn,InputTable1)
    rangepartition(InputTable2,5,openconnection,Table2JoinColumn,InputTable2)

    thread_1 = threading.Thread(target=threadJoinWork,args=('range_part_' + InputTable1 + '0', 'range_part_'
          + InputTable2 + '0',Table1JoinColumn,Table2JoinColumn,'partition_join0',openconnection))
    thread_2 = threading.Thread(target=threadJoinWork,args=('range_part_' + InputTable1 + '1', 'range_part_'
          + InputTable2 + '1',Table1JoinColumn,Table2JoinColumn,'partition_join1',openconnection))
    thread_3 = threading.Thread(target=threadJoinWork, args=('range_part_' + InputTable1 + '2', 'range_part_'
          + InputTable2 + '2',Table1JoinColumn,Table2JoinColumn,'partition_join2',openconnection))
    thread_4 = threading.Thread(target=threadJoinWork,args=('range_part_' + InputTable1 + '3', 'range_part_'
          + InputTable2 + '3',Table1JoinColumn,Table2JoinColumn,'partition_join3',openconnection))
    thread_5 = threading.Thread(target=threadJoinWork,args=('range_part_' + InputTable1 + '4', 'range_part_'
          + InputTable2 + '4',Table1JoinColumn,Table2JoinColumn,'partition_join4',openconnection))

    thread_1.start()
    thread_2.start()
    thread_3.start()
    thread_4.start()
    thread_5.start()

    thread_1.join()
    thread_2.join()
    thread_3.join()
    thread_4.join()
    thread_5.join()

    drop_stmt = 'DROP TABLE IF EXISTS ' + OutputTable
    cur.execute(drop_stmt)

    create_stmt = 'create table ' + OutputTable + ' as select * from ' + InputTable1 + ', ' + \
                  InputTable2 + ' where 1=0'
    cur.execute(create_stmt)

    for i in range(0,5):
        insert_stmt = 'insert into ' + OutputTable + ' (select * from ' + 'partition_join' + str(i) + ')'
        cur.execute(insert_stmt)

    openconnection.commit()


def rangepartition(tablename, numberofpartitions, openconnection, sortingColumn, inputTable):
    try:
        cur = openconnection.cursor()
        upper_bound = 0.0

        stat_stmt = 'select min(%s), max(%s) from %s' %(sortingColumn,sortingColumn,inputTable)
        cur.execute(stat_stmt)
        res = cur.fetchall()

        min_val = res[0][0]
        max_val = res[0][1]
        lower_bound = min_val
        range_width = (max_val - min_val)/ float(numberofpartitions)
        partition_tab_names = ['range_part_' + inputTable + str(i) for i in range(0,numberofpartitions)]

        for i in range(0,numberofpartitions):
            upper_bound = lower_bound + range_width

            drop_stmt = 'Drop table IF EXISTS range_part_' + inputTable + str(i)
            cur.execute(drop_stmt)

            if i is 0:
                create_stmt = 'create table ' + partition_tab_names[i] + \
                              ' as select * from ' + tablename + \
                              ' where ' + sortingColumn + ' >= ' + str(lower_bound) + ' and ' + \
                               sortingColumn + ' <= ' + str(upper_bound)
            else:
                create_stmt = 'create table ' + partition_tab_names[i] + \
                              ' as select * from ' + tablename + \
                              ' where ' + sortingColumn + ' > ' + str(lower_bound) + \
                              ' and ' + sortingColumn + ' <= ' + str(upper_bound)

            cur.execute(create_stmt)
            openconnection.commit()
            lower_bound += range_width

    except Exception:
        traceback.print_exc()

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
            openconnection.rollback()
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
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
