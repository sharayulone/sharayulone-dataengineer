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
df = df.na.fill(0)
df = df.na.drop()
df = df.na.fill({"salary" : 0})

## How do you perform join in PySpark?
df = df1.join(df2, df1.customer_id == df2.customer_id, "left")

## What is broadcast join?
df = df.join(broadcast(small_df), "id")

## What is repartition vs coalesce?
## repartition increases or decreases the number of partitions
## coalesce decreases the number of partitions

## How do you create UDF?
## Avoid UDF when possible — it bypasses Catalyst optimization.

## What are transformations and actions?
## Transformations are lazy - select, filter, join, groupBy they does not execute until an action is called on them.
## Actions - Trigger the execution - count, collect, write, show

## How do you optimize a slow Spark job?
## Answer structure:
##         Check Spark UI
##         Look for shuffle stages
##         Check skew
##         Increase partitions
##         Use broadcast join
##         Avoid UDF
##         Cache wisely
##         Tune executor memory

## How do you handle data skew?
## Techniques:
##           Salting
##           Broadcast small table
##           Increase partitions
##           Use skew join hints

## What is caching and persistence?
## cache() story level is only memory
## persist() we can define storage level, good for production jobs

## what is checkpointing 
## Checkpointing means saving the current state of your data to stable 
## storage (like HDFS or S3) so Spark doesn’t have to recompute everything from the beginning if something fails.

##SPARK SQL INTERVIEW QUESTIONS

## How do you create a temporary view?
df.createOrReplaceTempView("emp")

## Write Spark SQL query to get 2nd highest salary
sql_v1 = spark.sql(""" select max(salary) 
                      from employees
                      where salary < (select max(salary) from employees) """)

## Write Spark SQL query to get 2nd highest salary
sql_v2 = spark.sql(""" select max(salary)
                       from employees
                       where salary < (select max(salary) from employees) """)

## How to remove duplicates in Spark SQL?
spl = spark.sql(""" select distinct * from employees """)

## Find 2nd Highest Salary Using Window Function in sql 
sql_v3 = spark.sql(""" select salary from (select salary,
                              dense_rank(), over(order by salary desc) as rank from employees) t 
                              where rank = 2 """)
                        

## Find 2nd Highest Salary Using Window Function
window_spec = Window.orderBy(col("salary").desc())
df = df.withColumn("rank", dense_rank().over(window_spec)).filter("rank"==2).show()

## Find Top 3 Salaries Per Department
sql_v4 = spark.sql(""" select salary, department from (select salary,
                              department,
                              dense_rank(), over(partition by department order by salary desc) as rnk 
                              from employees ) t 
                              where rnk <= 3 """)

## Find Top 3 Salaries Per Department in pyspark
window_spec = Window.partitionBy(col("department")).orderBy(col("salary").desc())
df = df.withColumn("rank", dense_rank().over(window_spec)).filter(col("rank") <= 3)

## Remove Duplicate Records (Keep Latest) 
## Table: orders(id, status, updated_date)
sql_v5 = spark.sql(""" select id, updated_date from (
                           select id,
                           row_number() over(partition by id order by updated_desc desc) as rn from orders ) t
                           where rn = 1 """)

## Remove Duplicate Records (Keep Latest) in pyspark
## Table: orders(id, status, updated_date)

window_spec = Window.partitionBy("id").orderBy(col("updated_date").desc())
df = df.withColumn("rn", row_number().over(window_spec))
df_filter = df.filter(col("rn") ==1)

## Identify Duplicate Records
select id, count(*)
       from orders
       group by id
       having count(*) > 1;

## Identify Duplicate Records
df_dedup = df.groupBy("id")\
             .agg(count("*").alias("cnt"))\
             .filter(col("cnt") > 1)

## Find Employees Who Earn More Than Department Average
sql_v6 = spark.sql(""" select emp_id, 
                              emp_name, 
                              deparment,
                              sum(salary) over(partition by department
                              


























