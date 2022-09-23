from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_replace, col
import configparser
import os

config = configparser.ConfigParser()
config.read("/Users/samar/airflow/dl.cfg")
host=config['POSTGRES']['HOST']
port=config['POSTGRES']['PORT']
dbname=config['POSTGRES']['DBNAME']
user=config['POSTGRES']['USER']
password=config['POSTGRES']['PASSWORD']


def create_spark_session():
    """Creates the spark session"""

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL") \
        .config("spark.jars", "/Users/samar/airflow/postgresql-42.5.0.jar") \
        .getOrCreate()
    return spark


def process_sas_data(spark, input_data):
    """Reads immigration sas_data from parquet files and loads them to staging_immigartion table in Postgres database"""

    # get filepath to SAS data file
    sas_data = os.path.join(input_data, 'sas_data/')
    # read SAS data file
    df = spark.read.parquet(sas_data)
    # write data from spark dataframe to postgres
    df.write.mode("overwrite").jdbc(f"jdbc:postgresql://{host}:{port}/{dbname}", "staging_immigration",
          properties={"user": "samar", f"{user}": f"{password}", "driver": 'org.postgresql.Driver'})

def process_demographics_data(spark, input_data):
    """Reads demographics data, transforms the column names as per requirement and loads them to staging_demographics table in Postgres database"""

    # get filepath to demographics data file
    demographics = os.path.join(input_data, 'us-cities-demographics.csv')
    df = spark.read.csv(demographics, header=True, sep=';')
    # Renaming column names
    demographics = df.withColumnRenamed('City','city')\
                .withColumnRenamed('State','state')\
                .withColumnRenamed('Median Age','median_age')\
                .withColumnRenamed('Male Population','male_population')\
                .withColumnRenamed('Female Population','female_population')\
                .withColumnRenamed('Total Population','total_population')\
                .withColumnRenamed('Number of Veterans','no_of_veterans')\
                .withColumnRenamed('Foreign-born','foreign_born')\
                .withColumnRenamed('Average Household Size','avg_household_size')\
                .withColumnRenamed('Size','size')\
                .withColumnRenamed('State Code','state_code')\
                .withColumnRenamed('Race','race')\
                .withColumnRenamed('Count','count')
    # write data from spark dataframe to postgres
    demographics.write.mode("overwrite").jdbc(f"jdbc:postgresql://{host}:{port}/{dbname}", "staging_demographics",
          properties={"user": f"{user}", "password": f"{password}", "driver": 'org.postgresql.Driver'})

def process_airport_codes_data(spark, input_data):
    """Reads airport_codes data, transforms the column names as per requirement and loads them to staging_airport_codes table in Postgres database"""

    airport_codes = os.path.join(input_data, 'airport-codes_csv.csv')
    # Reading Airport code data
    df = spark.read.csv(airport_codes, header=True, inferSchema=True)
    airport_codes=df.withColumn('latitude', split('coordinates', ', ').getItem(0))\
    .withColumn('longitude', split('coordinates', ', ').getItem(1)) 

    # write data from spark dataframe to postgres
    airport_codes.write.mode("overwrite").jdbc(f"jdbc:postgresql://{host}:{port}/{dbname}", "staging_airport_codes",
          properties={"user": f"{user}", "password": f"{password}", "driver": 'org.postgresql.Driver'})

def process_global_temperature_data(spark, input_data):
    """Reads global temperature data, transforms the column names as per requirement and loads them to staging_global_temperature table in Postgres database"""

    global_temepraure = os.path.join(input_data, 'GlobalLandTemperaturesByCity.csv')
    df = spark.read.csv(global_temepraure, header=True, inferSchema=True)
    df = df.withColumnRenamed('dt','date')\
                .withColumnRenamed('AverageTemperature','avg_temperature')\
                .withColumnRenamed('AverageTemperatureUncertainty','avg_temperature_uncertainty')\
                .withColumnRenamed('City','city')\
                .withColumnRenamed('Country','country')\
                .withColumnRenamed('Latitude','latitude')\
                .withColumnRenamed('Longitude','longitude')
    # write data from spark dataframe to postgres
    df.write.mode("overwrite").jdbc(f"jdbc:postgresql://{host}:{port}/{dbname}", "staging_global_temperature",
          properties={"user": f"{user}", "password": f"{password}", "driver": 'org.postgresql.Driver'})
          
def process_i94_data(spark, input_data):
    """Reads I94_SAS_Labels_Descriptions data, \
        extracts & transforms i94 - address, country, port, mode, visa data from the file and \
            loads those values in tables staging_i94address, staging_i94country, \
                staging_i94port, staging_i94mode, staging_i94visa respectively in PostgreSQL database"""

    i94 = os.path.join(input_data, 'I94_SAS_Labels_Descriptions.SAS')
    df = spark.read.text(i94)
    i94country = df.filter("value like '% =  %'").withColumn('value', regexp_replace(col("value"), " =  ", "="))\
        .withColumn('value', regexp_replace(col("value"), "'", ""))\
        .withColumn('value', regexp_replace(col("value"), "   ", ""))\
        .withColumn('value', regexp_replace(col("value"), " ;", ""))\
        .selectExpr('substring(value, 0, 3) as code', 'substring(value, 5, length(value)) as country')
    # write data from spark dataframe to postgres
    i94country.write.mode("overwrite").jdbc(f"jdbc:postgresql://{host}:{port}/{dbname}", "staging_i94country",
          properties={"user": f"{user}", "password": f"{password}", "driver": 'org.postgresql.Driver'})
    
    i94port = df.filter("value like '%\t=\t%'").withColumn('value', regexp_replace(col("value"), "\t=\t", "="))\
        .withColumn('value', regexp_replace(col("value"), "'", ""))\
        .withColumn('value', regexp_replace(col("value"), "   ", ""))\
        .withColumn('value', regexp_replace(col("value"), " =", "="))\
        .selectExpr('substring(value, 0, 3) as code', 'substring(value, 5, length(value)) as port')
    # write data from spark dataframe to postgres
    i94port.write.mode("overwrite").jdbc(f"jdbc:postgresql://{host}:{port}/{dbname}", "staging_i94port",
          properties={"user": f"{user}", "password": f"{password}", "driver": 'org.postgresql.Driver'})
    
    i94mode = df.filter("value like '\t% = %'").withColumn('value', regexp_replace(col("value"), " = ", "="))\
        .withColumn('value', regexp_replace(col("value"), "\t", ""))\
        .withColumn('value', regexp_replace(col("value"), "'", ""))\
        .withColumn('value', regexp_replace(col("value"), " ;", ""))\
        .selectExpr('substring(value, 0, 1) as code', 'substring(value, 3, length(value)) as mode')
    # write data from spark dataframe to postgres
    i94mode.write.mode("overwrite").jdbc(f"jdbc:postgresql://{host}:{port}/{dbname}", "staging_i94mode",
          properties={"user": f"{user}", "password": f"{password}", "driver": 'org.postgresql.Driver'})
    
    i94address = df.filter("value like '\t%\\'=\\'%'").withColumn('value', regexp_replace(col("value"), " = ", "="))\
        .withColumn('value', regexp_replace(col("value"), "\t", ""))\
        .withColumn('value', regexp_replace(col("value"), "'", ""))\
        .withColumn('value', regexp_replace(col("value"), " ;", ""))\
        .selectExpr('substring(value, 0, 2) as code', 'substring(value, 4, length(value)) as state')
    # write data from spark dataframe to postgres
    i94address.write.mode("overwrite").jdbc(f"jdbc:postgresql://{host}:{port}/{dbname}", "staging_i94address",
          properties={"user": f"{user}", "password": f"{password}", "driver": 'org.postgresql.Driver'})
    
    i94visa = df.filter("value like '   _ =%'").withColumn('value', regexp_replace(col("value"), " = ", "="))\
        .withColumn('value', regexp_replace(col("value"), "   ", ""))\
        .selectExpr('substring(value, 0, 1) as code', 'substring(value, 3, length(value)) as type')
    # write data from spark dataframe to postgres
    i94visa.write.mode("overwrite").jdbc(f"jdbc:postgresql://{host}:{port}/{dbname}", "staging_i94visa",
          properties={"user": f"{user}", "password": f"{password}", "driver": 'org.postgresql.Driver'})


def main():
    spark = create_spark_session()
    print("===============================================================================")
    print("                         Spark connection configured!                          ")                         
    print("===============================================================================")
    input_data = '/Users/samar/airflow/data/'
    
    process_sas_data(spark, input_data) 
    print("staging_immigration table loaded successfully...")
    
    process_i94_data(spark, input_data) 
    print("staging_i94country loaded successfully...")
    print("staging_i94port loaded successfully...")
    print("staging_i94mode loaded successfully...")
    print("staging_i94address loaded successfully...")
    print("staging_i94visa loaded successfully...")  
    
    process_airport_codes_data(spark, input_data)
    print("staging_airport_codes table loaded successfully...")
    
    process_global_temperature_data(spark, input_data)
    print("staging_global_temperature table loaded successfully...")
    
    process_demographics_data(spark, input_data)
    print("staging_demographics table loaded successfully...")
    print("===============================================================================")
    print("                          Staging tables are ready!                            ")
    print("===============================================================================\n")

if __name__ == "__main__":
    main()
