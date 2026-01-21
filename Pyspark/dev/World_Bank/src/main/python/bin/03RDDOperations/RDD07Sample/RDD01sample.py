from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
	.appName('Set')\
	.master('local[*]')\
	.getOrCreate()

# sample is a transformantions
# sample will take random number fro RDD
# sample(withReplacement , fraction ,seed = None)
# withReplacement = True or False if true we can same data multiple time
# fraction 0 to 1  if 0.3 this take 30% of dataset to display . not gaurantee it will 30% around 30%
# seed = None if you paas 30 - two can have same random records.

rdd = spark.sparkContext.parallelize(range(100) ,4)

rdd1Smaple30 = rdd.sample(withReplacement= False,fraction=0.1 , seed =30)
print(f'First Seed =30 {rdd1Smaple30.collect()}')
rdd2Smaple30 = rdd.sample(withReplacement= False,fraction=0.1 , seed =30)
print(f'Second Seed =30 {rdd2Smaple30.collect()}')

print('-----------withReplacement = True (same records can be fetched again))----------------')

rdd3SampleReplament = rdd.sample(withReplacement = True , fraction = 0.1 , seed = 30)

print(rdd3SampleReplament.collect())