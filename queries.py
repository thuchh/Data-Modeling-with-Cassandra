
# Drop table before create these tables
drop_song_features = "DROP TABLE IF EXISTS song_features;"
drop_artist_song_by_user = "DROP TABLE IF EXISTS artist_song_by_user;"
drop_user_name = "DROP TABLE IF EXISTS user_name;"





# Create tables:
song_features = (""" CREATE TABLE IF NOT EXISTS song_features (
                                itemInSession int, sessionId int, artist text, 
                                song text, length float, 
                                PRIMARY KEY(itemInSession, sessionId) 
                                );
                 """)

artist_song_by_user = (""" CREATE TABLE IF NOT EXISTS artist_song_by_user (
                                 userId int, sessionId int, itemInSession int, 
                                 artist text, song text, firstName text, lastName text, 
                                 PRIMARY KEY ((userId, sessionId), itemInSession)
                                );
                      """)

user_name = (""" CREATE TABLE IF NOT EXISTS user_name (
                        song text, userId int, 
                        firstName text, lastName text, 
                        PRIMARY KEY (song, userId)
                        );
            """)





# Insert data into tables:

insert_data_song_features = (""" INSERT INTO song_features (
                                     itemInSession, sessionId, 
                                     artist, song, length
                                     )
                                VALUES (%s, %s, %s, %s, %s);
                            """)

insert_data_artist_song_by_user = (""" INSERT INTO artist_song_by_user (
                                             userId, sessionId, itemInSession, 
                                             artist, song, firstName, lastName
                                             ) 
                                      VALUES (%s, %s, %s, %s, %s, %s, %s);
                                  """)

insert_data_user_name = (""" INSERT INTO user_name (
                                song, userId, 
                                firstName, lastName
                                ) 
                            VALUES (%s, %s, %s, %s);
                        """)





# Use tables above to do 3 query tasks
query_1 = ("""
             SELECT artist, song, length 
             FROM song_features 
             WHERE itemInSession = %s AND sessionId = %s
          """)

query_2 = (""" 
              SELECT artist, song, firstName, lastName 
              FROM artist_song_by_user
              WHERE userId = %s AND sessionId = %s;
          """)

query_3 = (""" 
              SELECT firstName, lastName 
              FROM user_name 
              WHERE song = %s;
          """)





create_table_queries = [song_features, artist_song_by_user, user_name]
drop_table_queries = [drop_song_features, drop_artist_song_by_user, drop_user_name]
insert_table_queries = [insert_data_song_features, insert_data_artist_song_by_user, insert_data_user_name]