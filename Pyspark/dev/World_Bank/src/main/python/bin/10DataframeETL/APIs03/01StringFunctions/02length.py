from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , TimestampType
from pyspark.sql.functions import split , length

spark  = SparkSession.builder\
	.appName('String')\
	.master('local[*]')\
	.getOrCreate()

order_path = '/practice/retail_db/orders/part-00000'
schema = StructType(
[
	StructField("order_id" , IntegerType() , False),
	StructField("order_dt" , TimestampType() , False),
	StructField("cus_order_id" , IntegerType() , False),
	StructField("order_status" , StringType() , False),
]
)


df = spark.read.load(path = order_path ,
			format = 'csv' ,
			  sep = ',' ,
				schema = schema)
print('--------Working data-------------')
df.show(1)

#length
print('--------length-----------')
df.select(df.order_status ,length(df.order_status).alias("length_of_status")).show(10)
