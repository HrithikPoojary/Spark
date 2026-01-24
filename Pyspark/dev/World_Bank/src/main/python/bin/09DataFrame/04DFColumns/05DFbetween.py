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

#between is condition function this returns true or false

ord.select(ord.order_id.between(10,20).alias("Order_id")).show(20)

# to get the result we can use where function

ord.where(ord.order_id.between(10,20).alias("OrderId")).show(50)

ord[(ord.order_id.between(10,20).alias("OrderId"))].show(50)


spark.stop()
