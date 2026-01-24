from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType, TimestampType
from pyspark.sql.functions import lower , substring

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()

schema = StructType(
    [
        StructField('order_id' , IntegerType() , False),
        StructField('order_dt' , TimestampType(),False),
        StructField('customer_id' , IntegerType(),False),
        StructField('order_status' , StringType(),False)
    ]
)


ord = spark.read.load(path = '/practice/retail_db/orders' ,format = 'csv' ,sep = ',' , schema = schema)

data = [("Rober",35,40,40),("Robert",35,40,40) , ("Ram" , 31,33,29) , ("Ram" , 31,33,91)]

emp = spark.createDataFrame(data = data , schema = ('Name string ,score1 int , score2 int , score2 int'))

#sort
#sort is heavy operation it should through entire dataset to order it
#sortWithinPartitions  this locally partitions the dataset

ord.sort(ord.customer_id.asc()).show(5)

ord.sort(ord.order_dt.desc(),ord.customer_id.asc()).show(5)
# 0-descending 1-ascending
ord.sort(ord.order_dt , ord.customer_id , ascending = [0,1]).show(5)

data = [('a',1) , ('d',4) , ('c' ,3) , ('b' ,2),('e' ,5)]

df = spark.createDataFrame(data=data,schema = ('col1 string , col2 int'))
df.show()

df1 = df.repartition(2)

print('Number of partitions {df1.rdd.getNumPartitions()}')

for i in df1.rdd.glom().collect():
	print(i)
print('-------------Normal partition----------------')
df1.sort(df1.col1.asc()).show()
print('-----------sortWithinPartitions---------------')
df1.sortWithinPartitions(df1.col1.asc()).show()








spark.stop()
