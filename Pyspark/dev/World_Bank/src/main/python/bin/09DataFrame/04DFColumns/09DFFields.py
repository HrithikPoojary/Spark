from pyspark.sql import SparkSession  #type:ignore
from pyspark.sql.types import StructType,StructField,StringType,IntegerType  #type:ignore
from pyspark.sql import Row  #type:ignore

spark = SparkSession.builder\
        .appName("Columns")\
        .master('local[*]')\
        .getOrCreate()


# create struct fields

data = [ Row( r = Row(a1 = 1 , a2 = 'b')) , Row (r = Row(a1 = 2 , a2 = 'c'))]

df = spark.createDataFrame(data = data)

df.printSchema()

df.show()

print('---------Normal way to access the data------------')


df.select(df.r.a1).show()

print('-----------------getField--------------------') 

df.select(df.r.getField('a1')).show()

df1 = spark.createDataFrame([([1,2] , {'key' : 'value'})] , schema = ['lst' , 'dict'])

df1.printSchema()
df1.show()

print(' ---------------Normal way to access the data ----------------')

df1.select(df1.lst[1],df1.dict['key']).show()

print('--------------Using getItmes---------------------')

df1.select(df1.lst.getItem(1) , df1.dict.getItem('key')).show()


spark.stop()
