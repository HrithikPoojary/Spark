from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , TimestampType
from pyspark.sql.functions import split , length , trim , ltrim ,rtrim , lower ,upper ,initcap , lpad ,rpad

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


df.select(df.order_id,df.order_status).withColumn('pading_left' , lpad(col = df.order_id , len = 10 , pad= '*'))\
				      .withColumn('pading_right' , rpad(col = df.order_id ,len = 10 , pad = '#'))\
				      .withColumn('pading' , rpad(lpad(df.order_id , 5, '$'),10,'$'))\
.show()
