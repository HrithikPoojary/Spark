from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
        .master('yarn')\
        .appName('Python Spark Basic Example')\
        .getOrCreate()

print(f"Spark Session Created at {spark}")

spark.stop()