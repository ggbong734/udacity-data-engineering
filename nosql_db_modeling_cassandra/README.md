Completed on 5/28/2020 as part of Udacity Data Engineering Nanodegree program project 2_

# Project: Data Modeling with Apache Cassandra

Goal: create a Apache Cassandra database for an imaginary music streaming startup, Sparkify to address three specific queries. Since Cassandra databases are modeled on the queries we want to run, a table is created for each query. To process the local raw data (CSV files) and upload them to the database, Python *pandas* in Jupyter Notebook is used.

## Raw data 
Stored in `event_data` folder: 
* event data describes users's activities on Sparkify app such as playing music, going to home page, logging out, etc.

## Components
The file used in this project:
* `Project_2.ipynb` creates a new csv file creates analytics tables, and uploads them to S3 

---

## Database design
The design approach of the Cassandra database is to facilitate running these three queries:
1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = x, and itemInSession = x
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = x, sessionid = x
3. Give me every user name (first and last) in my music app history who listened to the song = x

## Steps in the ETL process:
* Combine all csv files into one large csv file
* Connect to a Cassandra instance on local machine and create a Keyspace
* With Cassandra, **tables in database are based on queries we want to run**
* A separate table is created to address each query
* Upload tables to local Cassandra cluster/instance

