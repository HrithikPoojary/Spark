from pyspark.sql import SparkSession

spark = SparkSession.builder\
	.appName('Conf')\
	.master('yarn')\
	.getOrCreate()


ord = spark.read.load(path = '/practice/retail_db/orders'
			,format = 'csv'
			,sep = ','
			,schema = ('order_id  int , order_dt timestamp , cis_order_id int , order_status string'))

orditems = spark.read.load('/practice/retail_db/order_items'
			   ,format = 'csv'
			   ,sep =','
			   ,schema = ('order_item_id int,order_item_order_id int,order_item_product_id int,quantity tinyint,subtotal float,price float '))

joined = ord.join(orditems , ord.order_id == orditems.order_item_order_id )

joined.explain()

print(joined.count())

print('Job Successfull')

#spark.stop()

# call using
# > spark-submit --master yarn --num-executors 5 05configurations.py
