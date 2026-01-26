from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField , StringType , IntegerType , TimestampType
from pyspark.sql.functions import date_add , current_date , current_timestamp ,next_day,last_day , dayofweek ,dayofmonth,dayofyear,weekofyear
from pyspark.sql.functions import col

spark = SparkSession.builder\
	.appName('Date')\
	.master('local[*]')\
	.getOrCreate()

path = '/practice/retail_db/orders/part-00000'

schema = StructType(
[
	StructField("order_id" , IntegerType() , False),
	StructField("order_dt" , TimestampType() , False),
	StructField("cus_order_id" , IntegerType() , False),
	StructField("order_status" , StringType() , False)
]
)

ord = spark.read.load(path = path , format = 'csv' ,sep = ',' ,schema = schema)

ord_date = ord.withColumn('new_order_dt' , date_add(ord.order_dt , 50))

df  = spark.createDataFrame(data =  [(' ' ,)] ,schema = ['id' , ])

ord.show(1)
ord_date.show(1)


df.select(current_date()).show()
df.select(current_timestamp()).show(truncate = False)
#next day of given weekday  ex - next_day(05-jan-26(Friday)  , Mon)   -> 08-jan-26
df.select(current_date().alias("date")).select(col("date") , next_day(col("date") , 'Fri')).show()

#last_day returns lasr day of the month

df.select(current_date().alias("date")).select(col("date") , last_day(col("date"))).show()

#week of day
df.select(current_date() , dayofweek(current_date())).show()

#date of month

df.select(current_date().alias("date")).select(col("date") , dayofmonth(col("date"))).show()

#day of year   ex 01-jan-25 -> 1  , 31-dec-25 ->365
df.select(current_date() , dayofyear(current_date())).show()

#week of the year 15-jan-25 -> 3 for better calender
df.select(current_date() , weekofyear(current_date())).show()

