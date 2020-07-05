_Completed on 5/25/2020 as part of Udacity Data Engineering Nanodegree program project 1_

# Project: Data Modeling with PostGreSQL

The goal is to develop a PostGreSQL database for an imaginary music streaming startup, Sparkify, and build an Extract-Transform-Load (ETL) pipeline using Python *pandas* on local JSON data and upload the data to the database.

Two types of data will be processed and stored in the database: 
* song data which describes the song title, year released, duration, artist information
* log data which describes the time, location, method, and characteristics of the subscriber each time a song is played

## Database design

The database schema is:
![ERdiagram](https://app.lucidchart.com/publicSegments/view/dedeaa66-82f1-49e6-932c-bf60fec624c8/image.png)

The database design uses the star schema where there is a central Fact table (**songplays**) which branches off to other Dimensions table containing more specific details of each song, artist, user, timestamp.

The design approach of the PostGreSQL database is to facilitate analytics on song plays using simple SQL queries.

Some example business-related questions that can be addressed easily by the **songplays** Fact table: 
* What is the ratio of songs played by users on free versus paid subscription?
* Which are the top 10 most popular artists/songs in the last month?
* How often does a user use the service? 
* Are users concentrated in several geographical areas(cities)?

---

## ETL process

The ETL pipeline performs the following tasks:
* Create the PostGreSQL database and tables based on the star schema
* Fetch song and log datasets from local JSON files
* Process the data using Python *pandas*
* Insert the data into the database

The ETL pipeline is stored as three Python scripts:
1. **create_tables.py** : drops existing tables and creates new tables
2. **etl.py** : extract JSON data, processes them, and upload into tables
3. **sql_queries.py**: contains SQL commands for creating, inserting into, and dropping tables

---

## Running the program

To start, first create the database by runing the **create_tables.py** script:
`python create_tables.py`
Then, perform the ETL process using the command:
`python etl.py`

--- 

## Other information

The Jupyter Notebook files are for sandboxing and testing purposes. 

Dataset information:
* Number of song files to process: 71 files
* Number of log files: 30 files (each one representing the transactions of one day) 
* Number of song plays (transactions): 6820 plays
* Number of users: 96
