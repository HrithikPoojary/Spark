from pyspark.sql import SparkSession #type:ignore
from pyspark.sql import Column #type:ignore
from pyspark.sql.types import StructType,StructField , StringType,IntegerType  #type:ignore
from pyspark.sql.functions import col #type:ignore

spark = SparkSession.builder\
	.appName('Column')\
	.master('local[*]')\
	.getOrCreate()

schema = StructType(
[
	StructField('order_id' , IntegerType() , False),
	StructField('order_dt' , StringType() , False),
	StructField('customer_id' , IntegerType() , False),
	StructField('order_status' , StringType() , False),

]
)
ord = spark.read.load(path= '/practice/retail_db/orders/part-00000', format = 'csv',sep =',' ,schema = schema)
#asc()
#desc()
#asc_nulls_first()
#asc_nulls_last()
#desc_nulls_first()
#desc_nulls_lst()


ord.orderBy(ord.order_status.asc()).show(5)

ord.orderBy(ord.order_status.asc()).select(ord.order_status).distinct().show()   # type distinct will not gauntee the orders

ord.select(ord.order_status).distinct().orderBy(ord.order_status.asc()).show()

spark.stop()

