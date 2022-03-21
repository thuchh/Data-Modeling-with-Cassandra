import os
import csv
import glob
import json
from cassandra.cluster import Cluster
from queries import *
from setups import (get_filepaths, create_eventdata, create_keyspace, drop_tables, create_tables)





def insert_data(session, file):
    
    print("\n [>] Get data and insert to prapared tables: ")
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip the header
        for line in csvreader:
            # collect data for table of query 1
            itemInSession, sessionId  = int(line[3]), int(line[8])
            artist, title, length = str(line[0]), str(line[9]), float(line[5])
            query_1_data = [itemInSession, sessionId, artist, title, length]

            # collect data for table of query 2
            userId, sessionId, itemInSession  = int(line[10]), int(line[8]), int(line[3])
            artist, song, firstName, lastName = str(line[0]), str(line[9]), str(line[1]), str(line[4])
            query_2_data = [userId, sessionId, itemInSession,  artist, song, firstName, lastName]

            # collect data for table of query 3
            song, userId = str(line[9]), int(line[10])
            firstName, lastName = str(line[1]), str(line[4])
            query_3_data = [song, userId, firstName, lastName]
            
            # insert to tables needed for query tasks
            session.execute(insert_table_queries[0], (query_1_data))
            session.execute(insert_table_queries[1], (query_2_data))
            session.execute(insert_table_queries[2], (query_3_data))
            
    print(" [>>] Successfully insert data")
    return session



    
def query_task_1(session, itemInSession, sessionId):
    print("\n [>] Query task 1: Give me the artist, song title and song's length in the music app history \ itemInSession=4 and sessionId=338 ")
    rows = session.execute(query_1, (itemInSession, sessionId))
    for row in rows:
        print(f'artist: {row.artist}, song: {row.song} length: {row.length:.8}')
    
    return session
    
    
    
def query_task_2(session, userId, sessionId):
    print("\n [>] Query task 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) \ userid = 10, sessionid = 182 ")
    rows = session.execute(query_2, (userId, sessionId))
    for row in rows:
        print(f'artist: {row.artist}, song: {row.song}, user first name: {row.firstname}, user last name: {row.lastname}') 
    
    return session
        

        
def query_task_3(session, song):
    print("\n [>] Query task 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own' ")
    rows = session.execute(query_3, (song,))
    for row in rows:
        #print(f'user first name: {row.firstname:>10},  user last name: {row.lastname}')  
        print(f'user first name: {row.firstname},  user last name: {row.lastname}')
        
    return session
                    


def main():
    file = 'event_datafile_new.csv'
    cluster, session = create_keyspace()
    
    # insert data from new created csv file to tables (in queries.py)
    insert_data(session, file)
    
    # query tasks
    query_task_1(session, itemInSession=4, sessionId=338)
    query_task_2(session, userId=10, sessionId=182)
    query_task_3(session, song='All Hands Against His Own')

#     # testing data
#     print('\n --------------- song_features -------------------')
#     rows = session.execute("select * from song_features;")
#     for row in rows[0:5]:
#         print(row)
#     print('\n --------------- artist_song_by_user -------------------')
#     rows = session.execute("select * from artist_song_by_user;")
#     for row in rows[0:5]:
#         print(row)
#     print('\n --------------- user_name -------------------')
#     rows = session.execute("select * from user_name;")
#     for row in rows[0:5]:
#         print(row)

    
    
if __name__ == "__main__":
    main()
