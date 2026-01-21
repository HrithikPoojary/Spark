from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder\
	.appName('local[*]')\
	.config('spark.jars.packages' , 'org.postgresql:postgresql:42.7.3')\
	.getOrCreate()



# We have to use numerical column for the numpartitions
jdbc_url = 'jdbc:postgresql://localhost:5432/spark'

properties = {
		'user' : 'spark_user',
		'password' : 'spark_password',
		'driver' : 'org.postgresql.Driver'
	}
query =  '''
	(select  o.*, row_number() over(order by order_id) as rn from orders o) t
'''


df = spark.read.jdbc(
		url = jdbc_url,
		table = query,
		column = 'rn',
		lowerBound = 500,
		upperBound = 1000,
		numPartitions = 10,
		properties = properties
	)

df.show()

spark.stop()


