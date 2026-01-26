#This some issue code needs to be checked

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder\
	.appName('regexp_extract')\
	.master('local[*]')\
	.getOrCreate()

df = spark.createDataFrame(data = [('11ss1 ab' , )] , schema = ['id' , ])

df.show()

#\d - only digit 0-9
#\w - only digit and alphabets
#\s - space
# + all the matching till non match occurs

# 11ss1 ab
# 1(only first match)
df.select(regexp_extract(df.id , '(\d)' , 1)).show()
#11ss1 ab
#11(s is not a digit)
df.select(regexp_extract(df.id , '(\d+)' , 1 )).show()

#idx - group (\d+) (\w+)
#		1    2

#11ss1 ab
#11 ->1
#  ss1 ->2
df.select(regexp_extract(df.id , '(\d+) (\w+)' , 2 )).show()

#11ss1 ab
#11 -> 1
#  ss1->2
#    ' '->3 

df.select( regexp_extract(df.id ,'(\d+) (\w+) (\s)' , 3 )).show()
#ab


df.select(
    regexp_extract("id", r'\d+([a-zA-Z]+\d+)', 1 ).alias("result")
).show()



















spark.stop()
