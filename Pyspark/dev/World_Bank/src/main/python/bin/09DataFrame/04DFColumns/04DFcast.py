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

# cast is used to covert data type
# cast() ==  asType()

print(ord.dtypes)

df = ord.select(ord.order_id.cast("string"))
print(df.dtypes)
spark.stop()
