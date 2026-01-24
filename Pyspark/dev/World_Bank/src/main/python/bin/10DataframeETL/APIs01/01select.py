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

print('---------------Normal way to access columns  -------------------')
ord.select(ord.order_id , 'order_id' , "order_id" , (ord.order_id + 10).alias('order10')).show(5)

print('-----------Dataframe Function on column----------------')
ord.select(ord.order_status , lower(ord.order_status)).show(5,truncate = False)



spark.stop()








