# Databricks notebook source
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

file_schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("LATITUDE", DoubleType(), True),
        StructField("LONGITUDE", DoubleType(), True),
        StructField("ELEVATION", DoubleType(), True),
        StructField("STATE", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("GSN_FLAG", StringType(), True),
        StructField("HON_CRN_FLAG", StringType(), True),
        StructField("WMO_ID", StringType(), True),
    ]
)

# Set the Blob storage account key and container name
storage_account = "cs210032002e66d12a5"
storage_account_key = "***"
container_name = "databrick"

# Set the CSV file name and path
file_name = "ghcnd-stations.txt"
file_path = (
    f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/{file_name}"
)
positions = [
    (1, 11),
    (13, 20),
    (22, 30),
    (32, 37),
    (39, 40),
    (42, 71),
    (73, 75),
    (77, 79),
    (81, 85),
]

# Read the fixed-width file into a PySpark DataFrame and extract substrings using withColumn()
df = spark.read.text(file_path)
for i, (start, end) in enumerate(positions):
    df = df.withColumn(f"col{i + 1}", df.value.substr(start, end - start + 1))
df = df.selectExpr(
    "col1 as ID",
    "CAST(col2 AS double) AS LATITUDE",
    "CAST(col3 AS double) AS LATITUDE",
    "CAST(col4 AS double) AS ELEVATION",
    "col5 AS STATE",
    "col6 AS NAME",
    "col7 AS GSM_FLAG",
    "col8 AS HCN_CRN_FLAG",
    "col9 AS WMO_ID",
)

# Convert the resulting RDD to a PySpark DataFrame using toDF()
df = df.rdd.map(lambda row: tuple(row)).toDF(schema=file_schema)

# Display the contents of the DataFrame
df.show()

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

# Define the regex pattern to extract non-numeric characters until the first number
pattern = "^[^0-9]+"

# Extract the non-numeric characters present in the ID column
df = df.withColumn("COUNTRY", regexp_extract(df.ID, pattern, 0))

# Display the contents of the DataFrame
df.show()

# COMMAND ----------

df.createOrReplaceTempView("table_name")
results = spark.sql("SELECT COUNTRY, GSN_FLAG, ID, NAME FROM table_name GROUP BY GSN_FLAG, COUNTRY, ID, NAME ORDER BY COUNTRY")
results.show()
