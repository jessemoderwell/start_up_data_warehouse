# Cloud Data Warehouse: Music Startup

Scripts to transform raw app data into database formatted for analytics using Python, AWS Redshift, and PostreSQL

## Table of contents

- General info
- Files Summary
- Technologies
- Setup
- Inspiratioin

### General Info
This database allows the startup to easily query their data for analytical purposes, rather than just for inventory and transaction tracking that operations, of which operations may use more. The design of the schema and tables is a star schema, which facilitates easy breakdown of the fact (in this case, when a song is played) across the dimensions of artist, song, user, and time. There is a table for the fact and each of these dimensions, with each table having unique Id's for each record (primary keys)

This schema works well for analytical queries because it is more denormalized than a 3rd normal form organization of this data, which saves time by eliminating the cost of many joins

### Files Summary
The data sources for the staging tables come from two s3 buckets; Log data comes in the form of jsons from 's3://udacity-dend/log_data', and song data comes in csv's from 's3://udacity-dend/song_data'. The etl script uses the copy command to transfer the data from those files

dwg.config holds the usernames, passwords, ARN resource, and aws redshift database endpoint for connection. sql_queries.py contains all the sql queries as strings for the other scripts to run. create_tables.py executes the queries that create the tables in our schema, while etl.py performs the etl (extract, transform, and load) from the log/ song data into the tables

### Technologies
Python 3.6.3
configparser 5.2.0
psycopg2 2.7.4

The technology we are using for our data warehouse, which is where our transformed analytical data resides, is AWS Redshift. Redshift uses PostgreSQL as it's query language, and incoming log and song data are housed in S3

### Setup
To run these scripts, execute in the terminal "python3 create_tables.py" first, then "python3 etl.py". The redshift cluster can be queried and tested in the query editor in the aws dashboard

### Inspiration
This project created for the Udacity Data Engineering Nanodegree course, specifically the Data Warehouse unit

