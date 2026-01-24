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

data = [("Robert",35,40,40),("Robert",35,40,40) , ("Ram" , 31,33,29) , ("Ram" , 31,33,91)]

emp = spark.createDataFrame(data = data , schema = ('Name string ,score1 int , score2 int , score3 int'))

ord.printSchema()
ord.show(5)

emp.printSchema()
emp.show()


#drop - to drop 1 or more columns
#dropDupplicates -  drops duplicate rows and optionally subset of columns


ord1 = ord.drop(ord.order_dt,ord.order_id)
ord1.show(3)


emp.dropDuplicates().show()
#drops random row out of 2 
emp.dropDuplicates(subset=['Name','score1','score2']).show()


spark.stop()
