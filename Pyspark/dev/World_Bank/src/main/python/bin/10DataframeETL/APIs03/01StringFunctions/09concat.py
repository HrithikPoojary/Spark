from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , TimestampType
from pyspark.sql.functions import concat,concat_ws ,lit

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
df.show(2)

#concat(*col)
#concat_ws(sep = '' , *col)  ->concat with seperator
df.select(df.order_id ,df.order_status , concat(df.order_id , lit(' ') , df.order_status),
					 concat_ws(' ' , df.order_id,df.order_status ))\
.show()
spark.stop()
