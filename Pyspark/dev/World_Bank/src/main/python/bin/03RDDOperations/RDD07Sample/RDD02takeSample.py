from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
	.appName('Set')\
	.master('local[*]')\
	.getOrCreate()

# takeSample is a Actions
# takeSample will take random number fro RDD
# sample(withReplacement , num ,seed = None)
# withReplacement = True or False if true we can same data multiple time
# num  if 10 it will give you fixed 10 sample results
# seed = None if you paas 30 - two can have same random records.

rdd = spark.sparkContext.parallelize(range(100) ,4)

print('--------------takeSample----------------')

print(f'Num = 10 {rdd.takeSample(withReplacement = True , num = 10  , seed = 10)}')

print(f'Num = 20 {rdd.takeSample(withReplacement = True , num = 20 , seed = 10)}')