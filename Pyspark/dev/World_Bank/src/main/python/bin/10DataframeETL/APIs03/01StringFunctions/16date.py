from pyspark.sql  import SparkSession
from pyspark.sql.functions import col,current_date,current_timestamp,months_between,date_add,date_sub,add_months,datediff,date_trunc

spark = SparkSession.builder\
	.appName("Date")\
	.master('local[*]')\
	.getOrCreate()


df = spark.createDataFrame(data = [(' ' , )], schema = ['id',])


#1.7.....  =  1 = 31 but some month has 30 , 28,31 it will give near to that value..
df.select(current_date().alias("date") , date_add(current_date(),50).alias("day_added"))\
  .select(col("date") , col("day_added"),months_between(col("day_added") , col("date") ).alias("months_between"))\
   .select(col("date") , col("day_added") , col("months_between") ,(col("months_between") * 31).alias("days_approxiamate") )\
.show()


#it better to keep roundOff as default True
df.select(current_date().alias("date") , date_add(current_date(),50).alias("day_added"))\
  .select(col("date") , col("day_added"),months_between(col("day_added") , col("date") ,roundOff = False ).alias("months_between"))\
.show()


# add
# sub (minus)
df.select(current_date().alias("date") , date_add(current_date(),2).alias("add") , date_sub(current_date() , 2).alias("sub")).show()

#add_months
df.select(current_date().alias("date") , add_months(current_date() , 2).alias("added_months")).show()

#datediff
df.select(current_date().alias("date") , date_add(current_date() , 49).alias("added_days"))\
  .select(col("date") , col("added_days") , datediff(col("added_days") , col("date")))\
.show()

#date_trunc

# 'yyyy' , 'mm' , 'day' , 'hour' , 'minute' , 'second' , 'week' ,'quarter'  etc

df.select(current_timestamp().alias("date"))\
   .withColumn('year' ,  date_trunc('yyyy' ,col("date")))\
   .withColumn('month' ,  date_trunc('mm' ,col("date")))\
   .withColumn('day' ,  date_trunc('day' ,col("date")))\
   .withColumn( 'hour' ,  date_trunc('hour' ,col("date")))\
   .withColumn( 'minute' , date_trunc('minute' ,col("date")))\
   .withColumn( 'second' , date_trunc('second' ,col("date")))\
.show(truncate = False)

















