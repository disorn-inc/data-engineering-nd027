# Project: Data Modeling with Postgres

## Sparkify Song Play Analysis

### Project goals

- Create a star schema with fact and dimension tables optimized for queries. 
- Build an ETL pipeline to transfer data from JSON log files to Postgres tables.

### Database purpose

Sparkify is not finding easy to analyse and extract valuable insights from log files.  The database will provide the analytic team an easy way to query their data.
### Database schema

Star schema was used to develop the database. It of one fact table _songplays_ and four dimensional tables _users, songs, artists, time_ this will aid simpler queries with faster aggregations.

#### Fact Table
`songplays` table: records in log data associated with song plays<br>
primary key: songplay_id<br>
`songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`


#### Dimension Tables
`users` table : users in the app<br>
primary key: user_id <br>
```user_id, first_name, last_name, gender, level```

`songs` table: songs in music database<br>
primary key: song_id<br>
`song_id, title, artist_id, year, duration`

`artists` table: artists in music database<br>
primary key: artist_id
`artist_id, name, location, latitude, longitude`

`time` table:timestamps of records in songplays broken down into specific units<br>
primary key: timestamp
`start_time, hour, day, week, month, year, weekday`

### ETL pipeline
We build ETL process by processing one data file in each dataset. Firstly, we process `song_data` by reading the original JSON files into Pandas DataFrame. Then we extract data and insert records into `songs` table and `artists` table. Secondly, we process `log_data` to extract data for `time` table, `users` table, `songplays` table.  The `songplays` is a little bit more complicated than others since need to query `songs` and `artists` to get the song ID and artist ID. 

After the ETL process has been successfully built, we build the pipeline to extract, transform, load the entire datasets.



### Project Files

The project includes six files:

1. `test.ipynb` is used to check if the records are successfully inserted.
2. `create_tables.py` is used to reset the tables in database
3. `etl.ypynb` reads and processes a single file from `song_data` and `log_data` and loads the data into tables.
4. `etl.py` reads and processes all files from `song_data` and `log_data` and loads them into tables.
5. `sql_queries.py` contains all sql queries, and is imported into the last three files above.
6. `data` folder contains `song_data` and `log_data`
7. `README.md`provides detailed description of the project.

###  How to Run

Below are steps you can follow to complete the project:

1. Run  `create_tables.py`  to create your database and tables or reset the database.
2. Run  `etl.py`  load ETL pipeline for processing of the data
3. Run  `test.ipynb`  to confirm the creation of your tables with the correct columns and to confirm that records were successfully inserted into each table. 






# data-engineering-nd027