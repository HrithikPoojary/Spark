from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType , IntegerType , TimestampType
from pyspark.sql.functions import substring ,substring_index

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


#substring(col , pos , len)
#substring_index(col , delim , count)
df.withColumn('order_year' , substring(df.order_dt , 1 , 4)).show()

df.withColumn("dummy01" , substring_index(df.order_dt , '-' , 1))\
  .withColumn("dummy02" , substring_index(df.order_dt , '-' ,2))\
  .withColumn("dummy03" , substring_index(df.order_dt , '-' ,3))\
.show()



spark.stop()
