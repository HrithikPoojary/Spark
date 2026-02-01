from pyspark.sql import SparkSession

spark = SparkSession.builder\
	.appName('JDBC Save')\
	.master("local[*]")\
	.config("spark.jars.packages" , "org.postgresql:postgresql:42.7.3")\
	.getOrCreate()


jdbc_url = 'jdbc:postgresql://localhost:5432/postgres'

properties = {
		'user' : 'spark_user',
		'password' : 'spark_password',
		'driver':'org.postgresql.Driver'
}

df = spark.read.load(path = '/practice/retail_db/orders/',format = 'csv'  , sep = ',' , 
				schema = 'orderid int , orderdt timestamp , cus_order_id int , order_status string')

df.write.jdbc(url = jdbc_url ,
		table = 'public.orders',
		  mode = 'overwrite',
		    properties = properties)

spark.stop()
