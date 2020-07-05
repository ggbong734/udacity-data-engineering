_This is one of the projects of Udacity's Data Engineer Nanodegree program | Completed on 6/14/2020_

# Project 4: Data Lake using PySpark and AWS S3

Objective: build an Extract-Transform-Load (ETL) pipeline using PySpark for a fictitious music streaming startup, Sparkify. 

The pipeline extracts information of songs and logs of songs played by users from an AWS S3 bucket, transform the data locally with PySpark, and upload a set of fact tables and dimensional tables following the star schema to a separate S3 bucket. 

The schema is designed to facilitate any downstream analytics team to find insights in the songs that Sparkify users are listening to.

Steps in the ETL process:
* Start a Spark session with Hadoop jar packages installed 
* Extract song and song log data from S3 using IAM id and access key
* Transform data by type conversion and joining datasets to create fact and dimensional tables 
* Upload tables to S3 to facilitate analytics on songs played

---

## Components
The files used in this project:
* `etl.py` is a script that pulls data from S3, creates analytics tables, and uploads them to S3 
* `dl.cfg` contains the credentials to access AWS


## Running the program:
Run the ETL process from the command line with the following line after switching to the `etl.py` file directory is:
```
python etl.py
```

Add your own AWS IAM access key id and secret access key to the `dl.cfg` file:
```
[AWS]
AWS_ACCESS_KEY_ID=<your_AWS_IAM_key_id>
AWS_SECRET_ACCESS_KEY=<your_AWS_IAM_access_key>
```

---

## Raw data
Song and songplay data are extracted from S3 buckets:
* song data: 's3://udacity-dend/song_data'. There are 14,896 songs in the database.
* user songplay data: 's3://udacity-dend/log_data'. There are 8,056 events in the database.

## Schema of final analytics tables
The raw data is transformed into a fact table and 4 dimensional tables with the following schema:

![Entity Relationship Diagram database](https://app.lucidchart.com/publicSegments/view/cd53774a-7e16-43a3-931a-65396c8ffd3e/image.png)

The **songplays** fact table contains records of songs played on the platform.

Each table is located in a separate directory on the S3 repository. 

---

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