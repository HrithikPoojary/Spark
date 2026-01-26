from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull,isnan ,nanvl,coalesce

spark = SparkSession.builder\
	.appName('Null')\
	.master('local[*]')\
	.getOrCreate()


df = spark.createDataFrame (data = [('Robert' , 1,None, 114.0) , ('John' , None , 2577 , float('nan'))] , schema = ['name' ,'id' ,'phone' , 'stAdd'])

df.show()

#isnull & isNull

df.select(df.phone , isnull(df.phone) , df.phone.isNull() ).show()

#isnan (Null != NaN)
df.select(df.stAdd , isnan(df.stAdd) , df.phone , isnan(df.phone) ).show()


#nanvl(works only for NaN) == nvl sql

df.select(df.stAdd , df.phone, nanvl(df.stAdd , df.phone)).show()
df.select(df.phone , df.stAdd , nanvl(df.phone , df.stAdd)).show()

#coalesce(works for null)
df.select(df.phone , df.stAdd , coalesce(df.phone , df.stAdd)).show()
df.select(df.stAdd , df.phone , coalesce( df.stAdd , df.phone)).show()


spark.stop()

