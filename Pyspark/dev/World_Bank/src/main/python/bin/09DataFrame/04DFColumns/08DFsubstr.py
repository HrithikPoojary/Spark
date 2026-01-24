from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql.types import StructType,StructField,StringType,IntegerType  #type:ignore

spark = SparkSession.builder\
	.appName("Columns")\
	.master('local[*]')\
	.getOrCreate()


schema = StructType(
[
	StructField('order_id' , IntegerType() , False),
	StructField('order_dt' , StringType() , False),
	StructField('customer_id' , IntegerType() , False),
	StructField('order_status' , StringType() , False)
]
)


df = spark.read.load(path ='/practice/retail_db/orders/' , format = 'csv' , sep = ',' ,schema = schema )

df.show(5,truncate=False)

df.printSchema()

df.where( (df.order_dt.substr(0,4) == '2013')   &   (df.order_status.contains('COMPLETE') )).show()
print(df.where( (df.order_dt.substr(0,4) == '2013')   &   (df.order_status.contains('COMPLETE') )).count())
	
spark.stop()
