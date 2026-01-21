from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
	.appName('Jdbc read')\
	.config('spark.jars.packages' , 'org.postgresql:postgresql:42.7.3')\
	.getOrCreate()

jdbc_url = 'jdbc:postgresql://localhost:5432/spark'
properties = {
		'user' : 'spark_user',
		'password' : 'spark_password',
		'driver' : 'org.postgresql.Driver'
	}

# Read all data

df = spark.read.jdbc(url = jdbc_url,
			table = 'public.orders',
			  properties = properties)

df.show(5)
df.printSchema()
