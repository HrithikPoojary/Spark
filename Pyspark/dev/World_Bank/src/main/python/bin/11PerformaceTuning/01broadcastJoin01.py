from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , FloatType , IntegerType



spark = SparkSession.builder\
	.appName('PerformanceTuning')\
	.master('local[*]')\
	.getOrCreate()

schema = StructType(
[
		StructField('order_item_id'       , IntegerType() , nullable = False),
		StructField('order_item_order_id' , IntegerType() , nullable = False),
		StructField('order_item_product_id' , IntegerType() , nullable = False),
		StructField('quantity' , IntegerType() , nullable = False),
		StructField('subtotal' , FloatType() , nullable = False),
		StructField('price' , FloatType() , nullable = False),
]
)



print("default")
# default mb of broadcast join
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
# default  10 MB we can increase upto 8GB



print("after manual configuration")
#to disable broadcast strategy
spark.conf.set("spark.sql.autoBroadcastJoinThreshold" , -1)
# to set normal = 10485760b


print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))


orddf = spark.read.load(path = '/practice/retail_db/orders_parquet' , format = 'parquet')

orditem = spark.read.load(path = '/practice/retail_db/order_items' , format = 'csv' , sep = ',',
			  schema = schema)


print('Orders')
orddf.show(5)
print('\nOrder items')
orditem.show(5)


joined = orddf.join(orditem , on = orddf.order_id == orditem.order_item_order_id , how = 'inner')

joined.show(5,truncate = False)

#joined.explain(True)
joined.explain()



















spark.stop()
