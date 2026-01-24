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

print(ord.order_id)
print(ord.columns)

# select  dataframe.columnname
print('-----------dataframe.columname---------------')
ord.select(ord.order_id,ord.order_status).show(5)

# select dataframe['columname']
print('------------dataframe["columname"]----------------------')
ord.select(ord['order_id'],ord['order_status']).show(5)

df = ord.select(ord.order_status)
df.show(1)

print('-------------col----------------------')
ord.select(col("*")).show(5,truncate = False)

print('------------col("singleColumn")------------------')
ord.select(col("order_status")).show(5)

spark.stop()

