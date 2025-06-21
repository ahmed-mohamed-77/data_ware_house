from pyspark.sql import SparkSession, types, Row
from pyspark.sql import functions as sf
from datetime import date, datetime
from typing import Iterator
import pandas as pd

# start spark session
spark=SparkSession.builder.appName("test_data_lake").getOrCreate()

csv_path = r"/mnt/c/Users/ghoniem/Downloads/7a5cf7b1-a53e-4840-9a4d-da1e0b1800fa.csv"

# Bronze Layer
# -------------------------------
# PySpark DataFrame
# Reading The DataFrame

# checking the datatypes of the column (Schema)
# check if i have not inferred the schema from the imported file

# PySpark will consider all the columns as string
# Selecting Columns and Indexing
# Check Description option similar to pandas

# original csv file
pyspark_df=spark.read.csv(path=csv_path, header=True, inferSchema=True)

pyspark_df.show()
pyspark_df.printSchema()

pyspark_df= pyspark_df.fillna({"col11":0})

pyspark_df.show()
pyspark_df.printSchema()

# Register a function as a UDF

def convert_integer_to_string(text):
    if text is None:   # Use None, not pd.isna (pandas not used here)
        return "7"
    return "7"


# register the function
convert_integer_to_string_udf = sf.udf(
    convert_integer_to_string, types.StringType()
)


# create new column or overwrite a the existing columns
# creating a testing instance of the file
pyspark_df_result = pyspark_df.withColumn(
    "test_col11", convert_integer_to_string_udf(sf.col("col11"))
)


pyspark_df_result.show()
pyspark_df_result.printSchema()


print(pyspark_df_result.take(1))
print(pyspark_df_result.tail(1))


"""
----------------------------------------------------
----------------- Test Data LakeHouse --------------
----------------------------------------------------
# define the config builder
initialize the config file
"""




# creating dataFrame
# create_df = spark.createDataFrame([
#     Row(a=1, b=2.0, c="string", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#     Row(a=1, b=2.0, c="string", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#     # Row(1, 2.0, "string", date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
# ])
# create_df.show()
# create_df.printSchema()


# df = spark.createDataFrame([
#     (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
#     (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
#     (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
# ],["id", "value", "text", "date_col", "datetime_col"])


# df.show()

# df.show(1, vertical=True)
# print("-"*50)
# df.show(2, vertical=True)
# df.show(truncate=True)

# df.select("id", "value", "text").describe().show()

# print("selecting only one columns")
# df.select("id").show()


# Silver Layers
# -------------------------------
# adding Columns 
# Dropping Columns


