from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType, TimestampType
from pyspark.sql.functions import lower , substring

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()


# union and unionAll(this will not remove the duplicates like sql)  both works same way
df1 = spark.range(10)
df2 = spark.range(5,15)

df1.union(df2).show(10)
df1.unionAll(df2).show(10)



df1 = spark.createDataFrame(data = [('a',1) , ('b',2)] ,schema = ('col1 string , col2 int'))
df2 = spark.createDataFrame(data = [(2,'b') , (3,'c')] ,schema = ('col2 int , col1 string'))

df1.show()
df2.show()


#df1.union(df2)  error > datatype mismatch	

print('------unionByName-----------')
df1.unionByName(df2).show()
print('--to get distinct--------')
df1.unionByName(df2).distinct().show()

#intersect and intersectAll(keeps all matched from both datasets)
df1 = spark.createDataFrame(data = [('a',1) , ('a' ,1) ,('b' ,2)] , schema = ('col1 string , col2 int'))
df2 = spark.createDataFrame(data = [('a' ,1) , ('a',1 ) ,('c' ,2)] , schema = ('col1 string , col2 int'))

print('----------instersect------------')
df1.intersect(df2).show()

print('---------intersectAll-----------------')
df1.intersectAll(df2).show()



#exceptAll ==sql minus operator
print('------exceptAll(minus)-----------')
df1 = spark.range(10)
df2 = spark.range(5,15)

df1.exceptAll(df2).show()








spark.stop()
