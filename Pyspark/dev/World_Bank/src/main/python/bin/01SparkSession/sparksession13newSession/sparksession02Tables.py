from pyspark.sql import SparkSession #type:ignore
from pyspark.sql.functions import udf  #type:ignore
from pyspark.sql.types import StringType,IntegerType #type:ignore


spark = SparkSession.builder\
	.appName('NewSession')\
	.master('local[2]')\
	.getOrCreate()

print(spark)
new_spark = spark.newSession()
print(new_spark)

spark.sql(""" create table students(id int)""")

spark.sql("""select count(*) from students""").show()

print('--------listTables---------------')
print(spark.catalog.listTables())
print('----------listColumns------------')
print(spark.catalog.listColumns('default.students'))

print('----------------We can access spark tables in new session-------------------------------------')

new_spark.sql("""select count(*) from students""").show()
