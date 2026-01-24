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


#withColumn takes two arguments  with(columname(alias) , col(column))
#withColumnRenamed takes two arguments to change the exsting column name


print('----------To add new column--------------')

ord.withColumn('order_year' , substring(ord.order_dt , 0,4)).show(5)

print('---------to modifiy the exsting column-------------')
ord.withColumn('order_dt' , substring(ord.order_dt,0,4)).show(5)

print('---------withColumnRenamed(  old column name   ,  new column name) ----------')

ord.withColumnRenamed('order_id' , 'order_idsssss').show(2)

