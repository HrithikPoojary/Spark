from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType, TimestampType
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()


df = spark.createDataFrame(data = [(1,100) ,(2,150) , (3,200) , (4,50) , (5,50)] , schema = ('id int , salary int'))

df.show()

# list = keeps the duplicates set does not
df.select(collect_list(df.salary).alias("list") , collect_set(df.salary).alias("set")).show(truncate=False)

print('----------To access the elements---------')
df.select(collect_list(df.salary)[1].alias('list') , collect_set(df.salary)[0].alias("set")).show()

print("-----Advance topic-------")
df.select(skewness(df.salary)).show()
df.select(stddev(df.salary)).show()
df.select(variance(df.salary)).show()
spark.stop()
