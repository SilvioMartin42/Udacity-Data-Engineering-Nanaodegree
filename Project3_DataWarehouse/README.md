# BEFORE USING:
Please launch your own redshift cluster and insert the appropriate pieces of information regarding ARN and HOST (= ENDPOINT) into **dwh.cfg**.

# How to run
To run the ETL process, please execute *create_tables.py* and afterwards *etl.py*, like this:

´´´
python create_tables.py
python etl.py
´´´

Run the *Example Analytics.ipynb* after the ETL process finished to connect to the database and have a look at the data in the tables and to execute some basic queries.

# Purpose of Database
The database helps Sparkify to analyse which users listen to which songs, how often, from where and during which time of the day.

# Schema design
Schema uses one fact table "songplays" and four dimension tables "users", "songs", "artists", "time". They are connected via (foreign) keys and form a **star schema**.

# ETL Process
Data is first loaded from S3 buckets into staging tables and consequently inserted into final tables. ***BEWARE:*** Loading the data into the staging tables takes around two hours!

# Errors 
During staging a total of 120 entries in the "songs" bucket could not be loaded into the "songs_staging" table due to bad data quality. To enable loading nevertheless, the optional command MAXERRORS 240 was included into the load-command for the staging_events_copy-command.