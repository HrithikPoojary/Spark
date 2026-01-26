from pyspark.sql  import SparkSession
from pyspark.sql.functions import col,current_date,current_timestamp,date_format , unix_timestamp , from_unixtime ,to_timestamp ,from_utc_timestamp,to_date , to_timestamp

spark = SparkSession.builder\
	.appName("Date")\
	.master('local[*]')\
	.getOrCreate()


df = spark.createDataFrame(data = [(' ' , )], schema = ['id',])


#coverting string  date format

df1 = df.select(date_format(current_date() , 'yyyy/MM/dd').alias("Date_change"))
df1.show()
df1.printSchema()


#unix_timestamp
df.select(current_timestamp().alias("timestamp") , unix_timestamp(current_timestamp()).alias("unix_timestamp") ).show(truncate = False)

#from_unixtimstamp
df3 =df.select(unix_timestamp(current_timestamp()).alias("unix_time"))
df3.select(df3.unix_time ,from_unixtime(df3.unix_time).alias("from_unix")).show()



#string to timestamp
df2 = df.select(current_timestamp().cast("string").alias("string_timestamp"))
df2.printSchema()

df2.select(to_timestamp("string_timestamp").alias('t')).printSchema()


#to convert to different timezone
df4 = spark.createDataFrame(data = [('1997-02-28 10:30:00' , 'JST')] , schema = ['timestamp' , 'timezone'])
df4.select(df4.timestamp , from_utc_timestamp(df4.timestamp , 'PST')).show(truncate=False)


#to_date ->convert to date format
#to_timestamp -> convert to timstamp format

df.select(current_timestamp() , to_date(current_timestamp() , 'yyyy-mm-dd')).show(truncate = False)


spark.stop()
