from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , TimestampType
from pyspark.sql.functions import instr , locate

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

#instr(col , substring)
df.select(df.order_status , instr(df.order_status , 'CL')).show()

#locate(substr , col , pos = 1) position is where to start
df.withColumn('locate02' , locate('00' , df.order_dt , 2)).show()
df.withColumn('locate13' , locate('00' , df.order_dt , 13)).show()
#2013-07-25 00:00:00
#123456789012345
#	    12
#	       15

spark.stop()
