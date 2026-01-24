from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType  #type:ignore
from pyspark.sql.functions import when #type:ignore


spark = SparkSession.builder\
        .appName("Columns")\
        .master('local[*]')\
        .getOrCreate()


schema = StructType(
[
	StructField('order_id' , IntegerType() , False),
	StructField('order_dt' , TimestampType() , False),
	StructField('customer_id' , IntegerType() , False),
	StructField('order_status' , StringType() , False),

]
)

df = spark.read.load(path = '/practice/retail_db/orders' , format = 'csv' , sep = ',' , schema = schema)

df.printSchema()
df.show(5)


df.select( df.order_status , when(df.order_status == 'CLOSED' , 'CL')\
			    .when(df.order_status == 'COMPLETE' , 'CO')\
			    .when(df.order_status == 'PENDING_PAYMENT' , 'PP')\
			    .when(df.order_status == 'PROCESSING' , 'PR')\
			    .otherwise(df.order_status).alias("status_shortcut")
  ).show()

spark.stop()

