
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql.functions import sumDistinct

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()


orderItems = spark.read.load(path = '/practice/retail_db/order_items/part-00000' , format = 'csv' , sep = ',' ,
                        schema = ('order_item_id int , order_item_order_id int , order_item_product_id int ,quantity int , subtotal float, price float'))
orderItems.show(5)


orderItems.select(sum(orderItems.price).alias("SumOfPrice") , sumDistinct(orderItems.price).alias("SumOfDistinctPrice")).show()



spark.stop()
