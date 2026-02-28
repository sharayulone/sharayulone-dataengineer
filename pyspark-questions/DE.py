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
df = spark.read.option("header", True)\
          .option("inferSchema", True)\
          .csv("file_path")
