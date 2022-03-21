import os
import cassandra
import glob
import csv
import pandas as pd
from cassandra.cluster import Cluster
from queries import *




def get_filepaths(subfolder):
    """
    Get path of files in subfolder
    """
    
    filepath = os.getcwd() + subfolder
    for root, dirs, files in os.walk(filepath):
            file_path_list = glob.glob(os.path.join(root,'*'))

    return file_path_list



def create_eventdata (file_path_list):
    """
    Get data from files in subfolder and write to file: event_datafile_new.csv
    """
    
    print(" \n [>] Create new datafile ")
    full_data_rows_list = [] 
    for f in file_path_list:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            csvreader = csv.reader(csvfile) 
            next(csvreader)   # skip the header of csv file

            # extracting each data row one by one and append it        
            for line in csvreader:
                full_data_rows_list.append(line) 

    # creating a smaller event data csv file called event_datafile_full.csv
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow([
            'artist', 'firstName', 'gender',
            'itemInSession', 'lastName', 'length',
            'level','location','sessionId','song','userId'
        ])
        
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow(
                (row[0], row[2], row[3], row[4], row[5], row[6],
                 row[7], row[8], row[12], row[13], row[16])
            )
            
    print(" [>>] Successfully create new datafile")


def create_keyspace():
    """
    Create keyspace: thuchh_cassandra
    """
    
    print(" \n [>] Create keyspace ")
    cluster = Cluster()
    session = cluster.connect()
    session.execute("""CREATE KEYSPACE IF NOT EXISTS thuchh_cassandra
                       WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
                    """)
    session.set_keyspace('thuchh_cassandra')
    print(" [>>] Successfully create keyspace: thuchh_cassandra")
    
    return cluster, session



def drop_tables(session):
    """
    drop each table using the queries in `drop_table_queries` list. 
    """
    print(" \n [>] Drop tables ")
    for query in drop_table_queries:
        session.execute(query)
        
    print(" [>>] Successfully drop tables")



def create_tables(session):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    
    print(" \n [>] Create tables ")
    for query in create_table_queries:
        session.execute(query)
    
    print(" [>>] Successfully create tables")
            
            
            
def main():
    subfolder_path = '/event_data'
    file_paths = get_filepaths(subfolder_path)
    create_eventdata(file_paths)
    cluster, session = create_keyspace()
    drop_tables(session)
    create_tables(session)
    
    session.shutdown()
    cluster.shutdown()



if __name__ == "__main__":
    main()

