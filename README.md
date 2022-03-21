# **Data Modeling with Cassandra**


## Data Modeling

The structure of this ETL pipeline follows:

*`event_data`* A folder with CSV files partitioned by date.

*`image_event_datafile_new.jpg`* directory with single image of the created CSV file (by `setups.py`).

*`queries.py`* all CQL queries create tables, insert datas and query to be used in Apache Cassandra.

*`setups.py`* contained Python function to transform the CSV file, create keyspace, tables needed for queries.

*`event_data_file_new.csv`* the CSV file processed by `setups.py`.

*`Project_1B_ Project_Template.ipynb`* a detailed description and a testing zone of the ETL pipeline solution.

*`etl.py`* contained functions to connect queries in queries.py with the Apache Cassandra and performs the ETL pipeline.


## Usage
To build the ETL:
Open terminal window on Jupyter Notebook and run commands:

1. ```python setups.py```
2. ```python etl.py```


## Example
`python etl.py` will produce the output:

```
 [>] Query task 1: Give me the artist, song title and song's length in the music app history \ itemInSession=4 and sessionId=338 
artist: Faithless, song: Music Matters (Mark Knight Dub) length: 495.30731

 [>] Query task 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) \ userid = 10, sessionid = 182 
artist: Down To The Bone, song: Keep On Keepin' On, user first name: Sylvie, user last name: Cruz
artist: Three Drives, song: Greece 2000, user first name: Sylvie, user last name: Cruz
artist: Sebastien Tellier, song: Kilometer, user first name: Sylvie, user last name: Cruz
artist: Lonnie Gordon, song: Catch You Baby (Steve Pitron & Max Sanna Radio Edit), user first name: Sylvie, user last name: Cruz

 [>] Query task 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own' 
user first name: Jacqueline,  user last name: Lynch
user first name: Tegan,  user last name: Levine
user first name: Sara,  user last name: Johnson
        
```