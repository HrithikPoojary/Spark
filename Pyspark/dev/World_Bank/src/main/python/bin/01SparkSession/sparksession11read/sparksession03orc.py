from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
        .master('local[2]')\
        .appName('ORC')\
        .getOrCreate()


'''
To create orc file


df = spark.read.load(path = '/practice/retail_db/orders'
			,format = 'csv'
			 ,sep = ','
			   , schema = ('order_id int ,order_date timestamp ,order_customer_id int ,order_status string'))

df.write\
    .mode('overwrite')\
	.option('compression','snappy')\
	   .orc('/practice/retail_db/orders_orc') 

'''

df = spark.read.load(path='/practice/retail_db/orders_orc' \
			, format = 'orc')

df.show(5,truncate = False)


spark.stop()
