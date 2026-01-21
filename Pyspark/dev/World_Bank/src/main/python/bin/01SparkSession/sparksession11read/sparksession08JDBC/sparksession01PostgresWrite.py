from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
	.appName('JDBC')\
	.config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3')\
	.getOrCreate()

df = spark.read.load(path = '/practice/retail_db/orders_parquet'\
			,format = 'parquet')


jdbc_url = 'jdbc:postgresql://localhost:5432/spark' 

properties = {
		'user' : 'spark_user',
		'password' : 'spark_password',
		'driver' : 'org.postgresql.Driver'
		}

df.write.jdbc(
		url = jdbc_url,
		table = 'public.orders',
		mode = 'overwrite',
		properties = properties

		)

