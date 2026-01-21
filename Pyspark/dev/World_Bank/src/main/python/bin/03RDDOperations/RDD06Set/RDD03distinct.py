from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
	.appName('Set')\
	.master('local[*]')\
	.getOrCreate()



rdd1 = spark.sparkContext.parallelize([1,2,3,3,3])
rdd2 = spark.sparkContext.parallelize([1,4,3])

rdd3 = rdd1.distinct()

print(f'Distinct elements {rdd3.collect()}' )   

