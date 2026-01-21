from pyspark.sql import SparkSession   #type:ignore
import os

spark = SparkSession.builder\
        .master('yarn')\
        .appName('Python Spark Basic Example')\
        .getOrCreate()

print(f"Spark Session Created at {spark}")

print('Number of partitons - ' + str(spark.conf.get('spark.sql.shuffle.partitions')))

spark.stop()