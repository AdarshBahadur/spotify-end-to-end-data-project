Spotify End-to-End Data Project
Introduction
In this project, we aim to build an ETL (Extract, Transform, Load) pipeline utilizing the Spotify API on AWS. The pipeline will efficiently extract data from the Spotify API, transform it to the desired format, and load it into the AWS Data Store.

Architecture Diagram

About Dataset/API
This API contains comprehensive information about music artists, albums, and songs - Spotify API

Services Used
S3 (Simple Storage Service): Amazon S3 is a highly scalable object storage service designed to store and retrieve any amount of data from anywhere on the web. It is commonly used for storing and distributing large media files, data backups, and static website files.

AWS Lambda: AWS Lambda is a serverless computing service that enables running code without managing servers. It can be configured to execute code in response to events like changes in S3, DynamoDB, or other AWS services.

CloudWatch: Amazon CloudWatch is a monitoring service for AWS resources and applications. It allows collecting and tracking metrics, monitoring log files, and setting alarms.

Glue Crawler: AWS Glue Crawler is a fully managed service for automatically crawling data sources, identifying data formats, and inferring schemas to create an AWS Glue Data Catalog.

Data Catalog: AWS Glue Data Catalog is a fully managed service for automatically crawling data sources, identifying data formats, and inferring schemas to create an AWS Glue Data Catalog that integrates with other AWS services, such as Athena.

Amazon Athena: Amazon Athena is an interactive query service facilitating easy data analysis in Amazon S3 using standard SQL. It can be used to analyze data in your Glue Catalog or other S3 Buckets.

Install Packages
bash
Copy code
pip install pandas
pip install numpy
pip install spotipy
Project Execution Flow
Extract Data from API
Lambda Trigger (Every 1 Hour)
Run extract code
Store Raw Data
Trigger Transformation Function
Transform Data and Load it
Query using Athena
