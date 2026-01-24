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


print('---------normal select ------------------')

ord.select(substring(ord.order_dt,0,4).alias("order_year")).show(5)

print('----------selectExpr  using sql like syntax----------------')

ord.selectExpr("substring(order_dt, 0 , 4) as order_year").show(5)

# few sql functions we can use in selectExpr but not in spark select like stack
# error > pyspark.sql.functions import stack
#dummy df

df  = spark.range(1)

df.selectExpr(" stack(3 , 1,2,3,4,5,6) ").show()





