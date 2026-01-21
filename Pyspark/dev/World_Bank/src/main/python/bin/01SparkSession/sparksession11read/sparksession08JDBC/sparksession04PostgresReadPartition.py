from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
	.appName('jdbc partition')\
	.config('spark.jars.packages','org.postgresql:postgresql:42.7.3')\
	.master('local[2]')\
	.getOrCreate()


# Partition should be done by numberical column like order_id 
jdbc_url = 'jdbc:postgresql://localhost:5432/spark'

properties = {
		'user' : 'spark_user',
		'password':'spark_password',
		'driver' : 'org.postgresql.Driver'
	}

df = spark.read.jdbc(
			url = jdbc_url,
			table = 'orders',
			column = 'order_id',
			lowerBound = 500,
			upperBound = 1000,
			numPartitions = 10,
			properties = properties
		   )
df.show(5)

'''
upperBound -lowerBound / numPartitions = 1000-500 = 500/10 = 50
stride = 50
partition 1 499+50 = 549 
partition 2  549 + 50 = 50
partition 3  599 + 50 = 50
partition 4  649 + 50 = 50
partition 5  749 + 50 = 50
partition 6  849 + 50 = 50
partition 7  949 + 50 = 50
partition 8  1049 + 50 = 50
partition 9  1149 + 50 = 50
partition 10  1249 +50 =  1349 - 68883 =  67534

'''

print(f'Number of partitons {df.rdd.getNumPartitions()}')

print('-----------------Records in each partitions-----------------------')

print(df.rdd.glom().map(len).collect())

spark.stop()
