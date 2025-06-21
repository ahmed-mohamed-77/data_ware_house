from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
import pandas as pd


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("FastSession") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .config("spark.ui.enabled", "true") \
    .getOrCreate()


df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])


print(df)

df.printSchema()
df.show()


df1 = spark.read.csv("/mnt/c/Users/ghoniem/Downloads/7a5cf7b1-a53e-4840-9a4d-da1e0b1800fa.csv", header=True, inferSchema=True)
df1.show()
