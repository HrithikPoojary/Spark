from pyspark.sql import SparkSession
from pyspark.sql.functions import abs,exp,factorial,sqrt,cbrt,pow,floor ,ceil,round,trunc,signum
from pyspark.sql.functions import lit,current_timestamp
spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()

data = [(-5 , 0) ,(1,3) ,(7,9)]
data2 = [(5.4 , 3) ,(7.9,1) ,(0.7,3)]
schema =  'col1 int , col2 int'
schema2 =  'col1 float , col2 int'
df = spark.createDataFrame(data = data , schema = schema)
df2 = spark.createDataFrame(data = data2 , schema = schema2)

df.show()

print('-----absolute-----')
df.select(df.col1 , abs(df.col1)).show()

print('-----exponential------')
df.select(df.col1 , exp(df.col1)).show()

print('-----factorial------')
df.select(df.col1 , factorial(df.col1)).show()

print('-----sqrt,cbrt,pow------')

df.select(df.col1,sqrt(df.col1) , cbrt(df.col1) , pow(df.col1 , 3)).show()


print('-------floor,ceil-------------')
df2.select(df2.col1,floor(df2.col1) , ceil(df2.col1)).show()


print('-----round-----------')
df.select( round(lit(1.66678) , 2)).show()

print('----trunc-----------')
df.select(trunc(current_timestamp() , 'yyyy')).show()
#we can use 'month' etc
#2013-07-25 -> 2013-07-00

#signum   value > n =>1 , value = 0  =>0 , value < n  => -1
print('--------signum---------')

#data = [(-5 , 0) ,(0,3) ,(7,9)]

df.select(df.col1, signum(df.col1)).show()

