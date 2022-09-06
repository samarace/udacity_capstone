# Udacity's Nanodegree Data Engineering: Capstone Project

---

## About

This project is a part of Udacity's Nanodegree Data Engineering program in which the participating students have to build an ETL process. The purpose of the data engineering capstone project is to give a chance to combine what is learned throughout the program and implement. For this project I have chosen to complete the capstone project provided by Udacity itself.

Here, there are four datasets to complete the project. The main dataset includes data on immigration to the United States, and supplementary datasets include data on airport codes, U.S. city demographics, and temperature data.

---

## Scope

In this project I have developed a data pipeline that creates an analytics database for information about immigration into the U.S. I have used AWS Redshift and Apache Airflow to complete the project.

---

## Data Gathering & Description

The following datasets were used to create the analytics database:

**I94 Immigration Data:** This data comes from the US National Tourism and Trade Office found here. Each report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).

**World Temperature Data:** This dataset came from Kaggle found here.

**U.S. City Demographic Data:** This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. Dataset comes from OpenSoft found here.

**Airport Code Table:** This is a simple table of airport codes and corresponding cities. The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the ICAO airport code which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia). It comes from here.

For programatic view, refer to *capstone-project.ipynb*

---

## Project Repository

The repository contains the following files & folders:

* **etl_dag.py:** creates the data pipeline that reads data from s3 and uses them to create the respective table in AWS Redshift Database.

* **dwh.cfg:** contains AWS credentials and other required configurations

* **capstone_project.ipynb:** python notebook for gathering, exploring and cleaning the provided datasets. These are then used to create the respective table in AWS Redshift

* **sql_queries:** All the drop and create queries are enlisted here

* **create_tables.py:** It imports and runs the sql_queries by setting up the necessary database connections

* **temp_data:** contains the world temperature dataset

* **sas_data:** contains parquet files

* **plugins:** contains helpers, operators and supporting files for the dag to execute error-free

* **images:** contains the ERD and graph view of the Airflow data pipeline

* **data:** contains the following files - airport-codes_csv.csv, I94_SAS_Labels_Descriptions.SAS, immigration_data_sample.csv, us-cities-demographics.csv

---

## Project Datasets

The dataset is stored in S3-bucket - 'udacity-capstone'

---

## Data Model

The data model consists of the following tables:

* immigration
* us_cities_demographics
* airport_codes
* world_temperature
* i94cit_res
* i94port
* i94mode
* i94addr
* i94visa

In immigration table i94mon column is used as a DISTKEY AND i94year as SORTKEY. The following tables are distributed across all nodes(DISTSTYLE ALL) - us_cities_demographics, i94cit_res, i94port, i94mode, i94addr, i94visa.

![Data Model](images/ERD_capstone.jpeg)

---

## Mapping Out Data Pipelines

Steps necessary to pipeline the data into the chosen data model:

1. Begin Dummy Operator
2. Operator extract tables from I94 labels mappings files and stage to S3/ local as csv:
    * i94cit_res
    * i94port
    * i94mode
    * i94addr
    * i94visa
3. Copy the above csv files from local/s3 to create tables in Redshift.
4. Perform data quality checks for the tables above.
5. Transform immigration data files on local/s3 and write results to `immigration` Redshift table
6. Perform data qualitiy checks for immigration table
7. Copy csv files from local/s3 to create the following tables in Redshift:
    * us_cities_demographics
    * airport_codes
    * world_temperature
    Perform data quality checks on above tables
8. End Dummy Operator

![Dag](images/dag_capstone.png)

---

## Run Pipelines to Model the Data

1. Create the data model
2. Build the data pipelines to create the data model
3. Create Tables:
    * (venv) $ python create_tables.py
4. Launch Airflow UI
5. Initialize Airflow & Run Webserver
6. Run Scheduler
7. Access Airflow UI
8. Run etl_dag in Airflow UI

---

## Technology Choices, tools and Q&A

1. Apache Airflow: Allows for easy scheduling and monitoring etl workflows for keeping analytics database up to date
2. Redshift: For storing analytics tables in a distributed manner

>> **Q&A:**
***Propose how often the data should be updated and why?***
Pipeline will be scheduled monthly as immigration data is the primary datasource is on a monthly granularity.
***Write a description of how you would approach the problem differently under the following scenarios:-***
***The data was increased by 100x:*** Will have to use partitioning functionality in the dag, might also need to use Cloud services like AWS EMR to use spark for processing data.
***The data populates a dashboard that must be updated on a daily basis by 7am every day:*** Will need to update the schedule of the DAG accordingly as make sure we have data needed for the dashboard.
***The database needed to be accessed by 100+ people:*** Will create roles for the different people on AWS. That way different people have access to the relevant resources.

---

help taken from:
<https://github.com/>
<https://knowledge.udacity.com/>
Udacity's mentors
