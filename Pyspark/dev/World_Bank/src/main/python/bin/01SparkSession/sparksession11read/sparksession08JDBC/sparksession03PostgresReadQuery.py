from pyspark.sql import SparkSession  #type:ignore

spark =  SparkSession.builder\
	.appName('Jdbc read')\
	.master('local[2]')\
	.config('spark.jars.packages' , 'org.postgresql:postgresql:42.7.3')\
	.getOrCreate()


jdbc_url = 'jdbc:postgresql://localhost:5432/spark'

properties = {
		'user' : 'spark_user',
		'password' : 'spark_password',
		'driver' : 'org.postgresql.Driver'
		}

query = """
(
  SELECT *
  FROM public.orders
  WHERE order_status = 'COMPLETE'
) t
"""


df = spark.read.jdbc(
		url = jdbc_url,
		table = query,
		properties = properties
)


df.show(10)


spark.stop()
