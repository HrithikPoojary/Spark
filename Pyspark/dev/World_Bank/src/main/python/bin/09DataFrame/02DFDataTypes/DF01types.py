from pyspark.sql import SparkSession  # type:ignore
from pyspark import StorageLevel #type:ignore


spark = SparkSession.builder\
	.appName('Storate')\
	.master('local[*]')\
	.getOrCreate()


spark.sql("""create table test(c1 string ,c2 varchar(10) , c3 char(10) )""")
spark.sql("""insert into test values('Robert','Robert','Robert')""")

spark.sql("""select * from test""").show()

spark.sql(""" select length(c1) as string_len , length(c2) varchar_len , length(c3) as char_len from test""").show()
