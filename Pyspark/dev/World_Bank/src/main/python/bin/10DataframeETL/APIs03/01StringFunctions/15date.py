from pyspark.sql  import SparkSession
from pyspark.sql.functions import col,current_date,current_timestamp,second,hour,minute,year,month,quarter

spark = SparkSession.builder\
	.appName("Date")\
	.master('local[*]')\
	.getOrCreate()


df = spark.createDataFrame(data = [(' ' , )], schema = ['id',])

df.select(current_date()).show()

#second -> returns integer
df.select(current_timestamp() , second(current_timestamp())).show(truncate= False)

df.select(current_timestamp() , hour(current_timestamp())).show(truncate = False)

df.select(current_timestamp() , minute(current_timestamp())).show(truncate = False)

df.select(current_timestamp() , month(current_timestamp())).show(truncate=False)

df.select(current_timestamp() , quarter(current_timestamp())).show(truncate=False)

df.select(current_timestamp() , year(current_timestamp())).show(truncate = False)

spark.stop()
