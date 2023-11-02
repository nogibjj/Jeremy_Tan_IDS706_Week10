"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well
"""
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return "stopped spark session"

def extract(
    url="""
   https://github.com/fivethirtyeight/data/blob/master/daily-show-guests/daily_show_guests.csv?raw=true 
    """,
    file_path="data/serve_times.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
 

    return file_path

def load_data(spark, data="data/serve_times.csv", name="DailyShowGuests"):
    """load data"""
    # data preprocessing by setting schmea
    schema = StructType([
        StructField("YEAR", IntegerType(), True),
        StructField("GoogleKnowlege_Occupation", StringType(), True),
        StructField("Show", DateType(), True),
        StructField("Group", StringType(), True),
        StructField("Raw_Guest_List", StringType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)
    return df


def query(spark, df, query, name): 
    """queries using spark sql"""
    df = df.createOrReplaceTempView(name)

    return spark.sql(query).show()

def describe(df):
    return df.describe().show()

def example_transform(df):
    """does an example transformation on a predefiend dataset"""
    conditions = [
        (col("GoogleKnowlege_Occupation") == "actor") | (col("GoogleKnowlege_Occupation") == "actress"),
        (col("GoogleKnowlege_Occupation") == "comedian") | (col("GoogleKnowlege_Occupation") == "comic"),
    ]

    categories = ["Acting", "Comedy"]

    df = df.withColumn("Occupation_Category", when(conditions[0], categories[0]).when(conditions[1], categories[1]).otherwise("Other"))

    df.show()

    return 


