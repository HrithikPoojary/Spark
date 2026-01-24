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
#contains()
#like()
#rlike()
#startswith()
#endswith()


print('---------contains--------------')
ord.where(ord.order_status.contains("CANCELED")).show()

ord.where(ord.order_status.contains("CANCELED")).select(ord.order_status).distinct().show()

print('----------like------------')

ord.where(ord.order_status.like('%O%')).show(10)

print('--------startswith------------')

ord.where(ord.order_status.startswith("CAN")).select(ord.order_status).distinct().show()

# in contains we can check only one records to get multiple records we can you isin()

ord.where(ord.order_status.isin("CANCELED" , "CLOSED")).select(ord.order_status).distinct().show()

spark.stop()
