from pyspark.sql import SparkSession

spark = SparkSession.builder\
	.appName('Repartition')\
	.master('local[*]')\
	.getOrCreate()


df = spark.read.load(path = '/practice/test/', format = 'csv' , sep = ',' , schema = 'col1 int , col2 int , col3 int')

print(f"Row Number - {df.count()}")

print(f"Before filter - {df.rdd.getNumPartitions()}")

df_filter = df.where(df.col1<501)

print(f"After filter - {df_filter.count()}")

print(f"After filter - {df_filter.rdd.getNumPartitions()}")

spark.stop()

