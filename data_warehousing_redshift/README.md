_This is one of the projects of Udacity's Data Engineer Nanodegree program | Completed on 5/31/2020_

# Project 3: Data Warehousing in S3 and Redshift

The objective of the project is to build an Extract-Transform-Load (ETL) pipeline in Python and SQL for a fictitious music streaming startup, Sparkify. 

The pipeline will extract song data and information on songs played by users from an AWS S3 bucket, stage the data in AWS Redshift, and transform the data into a set of dimensional tables following the star schema. 

The schema is designed to facilitate any downstream analytics team to find insights in what songs Sparkify users are listening to.

There are several steps in the ETL process:
* Start a Redshift cluster 
* Create an IAM Role with read and write access on Redshift and read access on S3, and attach the role to the Redshift cluster
* Create the staging tables and final dimensional tables in Redshift
* Copy song and songplay data from S3 buckets and place them at the staging tables on Redshift
* Transform the data by transferring them to the dimensional tables

---

## Raw data
Song and songplay data are extracted from S3 buckets:
* song data: 's3://udacity-dend/song_data'. There are 14,896 songs in the database.
* user songplay data: 's3://udacity-dend/log_data'. There are 8,056 events in the database.


## Staging tables
Then the data is stored in staging tables in Redshift with the following schema:

![Entity Relationship Diagram staging](https://app.lucidchart.com/publicSegments/view/e0886870-ca8c-4181-8bf2-a086d7e32ff7/image.png)

The **staging_events** table holds information on what songs users have played.
The **staging_songs** table contains information about songs and the artists.

## Fact and dimension tables
Data from the staging tables are then transformed into a fact table and 4 dimensional tables with the following schema:

![Entity Relationship Diagram database](https://app.lucidchart.com/publicSegments/view/cd53774a-7e16-43a3-931a-65396c8ffd3e/image.png)

The **songplays** fact table contains records of songs played on the platform.

---

## Running the program
The program has two scripts and two files:
* **create_tables.py**: This script drops existing tables and creates the staging, fact, and dimension tables.
* **etl.py**: This script extracts data from S3 buckets, loads them onto staging tables on Redshift, and transform into the fact and dimension tables.
* **sql_queries.py**: This file has all the SQL commands and queries used by the two scripts above.
* **dwh.cfg**: This text file contains the Redshift cluster endpoint address, database name, username, password, port number, ARN of IAM role, and addresses of S3 buckets.

To initiate the ETL process, first run `python create_tables.py` in the command line.

Once completed, run `python etl.py` to load the staged data to the fact and dimension tables.

Duplicate records are excluded by incorporating the `DISTINCT` function in SQL when loading the data to the fact and dimension tables.

Redshift does not support `UPSERT` statements such as `ON CONFLICT DO/UPDATE` so they can't be used when loading.

### Example queries
The resultant fact and dimension tables facilitate queries such as :

Get the top 5 most popular songs:
`SELECT s.title, COUNT(songplay_id) 
FROM songplays sp
JOIN songs s
ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY COUNT(songplay_id) DESC
LIMIT 5;`

Get the top 5 most popular artists:
`SELECT a.name, COUNT(*) 
FROM songplays sp
JOIN artists a
ON sp.artist_id = a.artist_id
GROUP BY a.name
ORDER BY COUNT(*) DESC
LIMIT 5;`

Top 5 regions with the most songs played by users:
`SELECT location, COUNT(songplay_id) 
FROM songplays
GROUP BY location
ORDER BY COUNT(songplay_id) DESC
LIMIT 5;`