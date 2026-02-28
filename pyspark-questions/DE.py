## What is the difference between RDD, DataFrame?
##-> RDD are low level api and it does not contain schema
## -> dataframes are distributed with schema (table)

## What is catalyst optimizer?
## -> Catalyst Optimizer is Spark SQL’s query optimization engine that converts logical plans into 
##  optimized physical execution plans using rule-based and cost-based optimizations.

## what is tungsten optimizer?
## -> Tungsten is Spark’s execution engine that improves performance by managing memory efficiently, processing data in 
## binary format, and using whole-stage code generation to reduce CPU and garbage collection overhead.

## How do you read a CSV file in PySpark?
df = spark.read.option("header", "true")\
          .option("inferSchema", "true")\
          .csv("file_path")

## How do you filter rows?
df_filtered = df.filter(df.age > 25)
## Or
from pyspark.sq.functions import *
df_filtered = df.filter(col("age") > 25)

## Difference between select() and withColumn()?
## --> select() will select only the required columns from a dataframe, suppose there are 20 columns returning from a
## dataframe and we need only 3 columns, then we can use select() to select only the required columns.
## withColumn() will add new column or it will modify an existing column.

## Write a code which will increase the salary by 10% 
--> df = df.withColumn("new_salary", df.salary * 1.1)

## How do you remove duplicates?
df_dedup = df.dropDuplicates()
## Or
df_dedup = df.dropDuplicates(["customer_id"])

## Difference between groupByKey() and reduceByKey()?
## reduceByKey() performs map-side aggregation before shuffle, making it more efficient, whereas groupByKey() 
## shuffles all values for a key without pre-aggregation, which increases memory and network overhead.

## How do you handle null values?

























