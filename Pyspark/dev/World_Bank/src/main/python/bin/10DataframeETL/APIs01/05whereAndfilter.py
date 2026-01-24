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

ord.printSchema()
ord.show(5)

emp.printSchema()
emp.show()

#filter == where

ord.select( ((ord.order_id > 10) & (ord.order_id < 20)).alias('good') ).show(5)
print('-------where and(&)-----------')
ord.where( ( (ord.order_id > 10 ) & (ord.order_id < 20)) ).show(5)

print('-------or(|)------------')
ord.where((ord.order_status.contains('CLOSED')) | (ord.order_status.contains('COMPLETE'))).show()

ord.where( ord.order_status.isin('COMPLETE' , 'CLOSED')).show(5)
ord.where( "order_status in ('COMPLTE' , 'CLOSED')" ).show()





spark.stop()
