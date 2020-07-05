_This is one of the projects of Udacity's Data Engineer Nanodegree program | Completed on 7/2/2020_

# Project 5: Data Pipelines with Airflow

Objective: build an automated and monitoring data pipeline using Airflow for a fictitious music streaming startup, Sparkify. 

The pipeline extracts information of songs and logs of songs played by users from an AWS S3 bucket, stages the data on Redshift, creates a set of fact tables and dimensional tables following the star schema in Redshift, and automate the process to run every hour with Airflow.  

The Airflow DAG is set to run every hour and has 5 different operators as shown below: 
* Stage data: Extract song and song log data from S3 and upload to staging tables on Redshift using Airflow and SQL
* Create tables: Create new fact and dimensions tables in Redshift
* Load fact table: Insert data from staging tables to fact table
* Load dimension tables: Insert data into staging tables to dimension tables
* Run data quality checks: Perform data quality checks and inform user if unexpected result is observed

Airflow task dependencies:
![Airflow task dependencies](example-dag.png?raw=true "Airflow task dependencies")
---

## Components
The files used in this project:
* `dag/udac_example_dag.py` is the main Airflow script that runs and sets task dependencies of the operators described above
* `plugins/operators/load_fact.py` defines the Airflow operator that inserts data into Redshift fact table
* `plugins/operators/load_dimension.py` defines the Airflow operator that inserts data into Redshift dimension tables
* `plugins/operators/stage_redshift.py` defines the Airflow operator that extracts data from S3 and uploads them to Redshift 
* `plugins/operators/data_quality.py` defines the Data Quality Monitoring Operator in Airflow

Helper files:
* `dag/create_tables.sql` SQL script to create fact and dimension tables in Redshift
* `plugins/helpers/sql_queries.py` has all the SQL code to insert data into the fact and dimension tables

## Running the program:
Set up a new cluster in AWS Redshift in us-west-2 region.

Create 'aws_credentials' AWS connection in Airflow with IAM key and secret key

Create 'redshift' Postgres connection in Airflow with with database name, database url, user name, password, and port number 

Run the Airflow DAG script:
```
python dag/udac_example_dag.py
```

---

## Raw data
Song and songplay data are extracted from S3 buckets:
* song data: 's3://udacity-dend/song_data'. There are 14,896 songs in the database.
* user songplay data: 's3://udacity-dend/log_data'. There are 8,056 events in the database.