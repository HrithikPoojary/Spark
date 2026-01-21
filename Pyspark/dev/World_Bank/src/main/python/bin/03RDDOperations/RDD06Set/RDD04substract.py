from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
	.appName('Set')\
	.master('local[*]')\
	.getOrCreate()



rdd1 = spark.sparkContext.parallelize([1,2,3,3,3,8])
rdd2 = spark.sparkContext.parallelize([1,4,3,7])

rdd3 = rdd1.subtract(rdd2)
rdd4 = rdd2.subtract(rdd1)

print(f'Subtract rdd1 - rdd2  elements {rdd3.collect()}' )  

print(f'Subtract rdd2 - rdd1  elements {rdd4.collect()}' )  