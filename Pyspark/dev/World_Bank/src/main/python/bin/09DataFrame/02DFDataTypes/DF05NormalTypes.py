from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder\
	.appName('NaN')\
	.master("local[2]")\
	.getOrCreate()


# For NaN (not a number) we have to assign this to float or double

df = spark.sql("select 'NaN' as col")

df.printSchema()

df1 = spark.sql("select double('NaN') as col")

df1.printSchema()

# Two NaN types are always true
spark.sql("select double('NaN') = float('NaN') as col").show()

# To check NaN will include in the aggregate  we will take NaN as normal val

spark.sql(""" create table dummy1(c1 int , c2 double )""")

spark.sql(""" insert into dummy1 values(1,10.0)""")
spark.sql(""" insert into dummy1 values(1,double('NaN'))""")
spark.sql(""" insert into dummy1 values(1, 10.0)""")
spark.sql(""" insert into dummy1 values(1,double('NaN'))""")
spark.sql(""" insert into dummy1 values(1,double('NaN'))""")

spark.sql("""select c2 , count(*) as count from dummy1 group by c2""").show()

spark.stop()
