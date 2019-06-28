
# Project Name: Data Modeling on Cloud Datawarehouse

###### <strong> Project Overview: <strong>
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They are planning to move their process on to the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app . 

###### <strong> Project Aim: <strong>
Sparkify like a to hire a data engineer to create ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. The role of a data engineer is mainly to determine what data model to use and create a data model specific database schema and also appropriate ETL pipeline to bring the data from json files in s3 to amazon redshift database tables for sparkify to do their required analysis. 

###### <strong> Project Description
After thoroughly reading through the requirement and understanding the data and needs of sparkify, for analysis team to understand the user activity on the app they need necessary statistics of the activities and the results that are retrieved should be fast and accurate. The primary reason dimensional modeling is its ability to allow data to be stored in a way that is optimized for information retrieval once it has been stored in a database.Dimensional models are specifically designed for optimized for the delivery of reports and analytics.It also provides a more simplified structure so that it is more intuitive for business users to write queries. Tables are de-normalized and are few tables which will have few joins to get the results with high performance. 

###### <strong> Dimensional Modelling: 
A dimensional model is also commonly called a star schema.The core of the star schema model is built from fact tables and dimension tables. It consists, typically, of a large table of facts (known as a fact table), with a number of other tables surrounding it that contain descriptive data, called dimensions. 

###### <strong> Fact Table: 
The fact table contains numerical values of what you measure. Each fact table contains the keys to associated dimension tables. Fact tables have a large number of rows.The information in a fact table has characteristics. It is numerical and used to generate aggregates and summaries. All facts refer directly to the dimension keys. Fact table that is determined after carefull analysis which contains the information.Fact table will have data where page column listed as "NextSong" 

###### <strong> Tables (Facts)
Table Name: Songplay(fact)
Column Names: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


###### <strong> Dimension Tables: 
The dimension tables contain descriptive information about the numerical values in the fact table. 

###### <strong> Tables ( Dimensions )

Table Name:Songs (Song Dimension)
Column Names: song_id, title, artist_id, year, duration

Table Name: Artists (Artist Dimension)
Column Names: artist_id, name, location, lattitude, longitude

Table Name:Users (User Dimension)
Column Names: user_id, first_name, last_name, gender, level

Table Name: Time (Time Dimension)
Column Names: start_time, hour, day, week, month, year, weekday


###### <strong> Pre-requisites cluster setup:
An Amazon redhift cluser should be created and able to connect when using any tool. I have used Aginity workbench. Place the host name and other parameters in the dwh.cfg file. 
I have creates 8 dc2.Large instances to get my etl script running, took 5 minutes to compelte everything 

    
###### <strong> Database creation Approach: 
Implement all the database DDL and DML statements in the sql_queries.py that create all the dimension tables and fact tables with all necessary columns. Build an ETL pipeline to extract data from the Json files from s3 and  push the data into necessary staging tables and then use the staging tables to insert appropriate data into dimensions and fact tables. 

 
###### <strong> Database Design scripts : 
create_tables.py : This is the first script that need to be executed. It takes all the Drop table variables list and drops the tables if exists and creates the tables with the help of DDL statements that are imported from sql_queries.py file. 

sql_queries.py: This file contains all the DDL and DML statements that are necessary to drop,create tables and Insert data when it is being retrived form the json file and being pushed to the tables in ETL pipeline.

###### <strong> Unit Test approach: 
Once the data loading part is done in staging table. I have validated connecting to redshift using aginity workbench. 

ETL Pipeline Script:
etl.py: This is the next script that is executed which grabs each file and process it and loads data into songs, artists, users dimensions. Then the time dimension is being loaded using the time stamp column. Fact tables are loaded finally to the complete the whole loading process.

# SQL queries for Analysis: 

Scenario 1: Sparkify want to analyse how many users are paid and free please find the Query to get the results. 

SELECT u.level, count(distinct u.user_id) Account_type  
FROM songplays sp join users u on sp.user_id = u.user_id 
group by u.level

Scenario 2: sparkify want to analyse how many times the user accessed the apps, to get the activity count please use below query

SELECT u.level, u.first_name,u.last_name , count(distinct session_id) user_activity  
FROM songplays sp join users u on sp.user_id = u.user_id 
group by u.level,u.first_name,u.last_name

Scenario 3 : sparkify want to analyse how many uses are accessing the app on the monthly and weekly basis. 

SELECT t.month , count(distinct u.user_id) user_activity  
FROM songplays sp join users u on sp.user_id = u.user_id  join time t on sp.start_time = t.start_time 
group by t.month


SELECT t.month,t.week , count(distinct u.user_id) user_activity  
FROM songplays sp join users u on sp.user_id = u.user_id  join time t on sp.start_time = t.start_time 
group by t.month,t.week





