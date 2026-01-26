from pyspark.sql import SparkSession
from pyspark.sql.window import  Window
from pyspark.sql import window
from pyspark.sql.functions import sum


spark = SparkSession.builder\
        .appName('Window')\
        .master('local[*]')\
        .getOrCreate()

#print(help(window))
'''
data = [
    ('James' , 'Sales','NY' , 9000 , 34),
    ('Alicia' , 'Sales','NY' , 8600 , 56),
    ('Robert' , 'Sales','CA' , 8100 , 30),
    ('John' , 'Sales', 'AZ' , 8600 , 31),
    ('Rahul' , 'Sales', 'AZ' , 1000 , 31),
    ('Ross' , 'Sales','AZ' , 8100 , 33),
    ('Lisa' , 'Finance','CA' , 9000 , 24),
    ('Deja' , 'Finance','CA' , 9900 , 40),
    ('Sugie' , 'Finance','NY' , 7900 , 36),
    ('Charlie' , 'Finance','AZ' , 5000 , 36),
    ('Ram' , 'Finance','NY' , 8900 , 53),
    ('Kyle' , 'Marketing','CA' , 8000 , 25),
    ('Reid' , 'Marketing','NY' , 9100 , 50)
]

schema = ['empname', 'dept','state','salary','age']

df = spark.createDataFrame(data = data ,schema = schema)
'''

data = [(101 , 1),(101,2),(101,3),(101,4),(101,5)]
df = spark.createDataFrame(data = data ,schema = ("dept int , salary int"))



print('------------unboundedPreceding , unboundedFollowing----------------------------------')
specRange = Window.partitionBy(df.dept).orderBy(df.salary)\
		  .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)


specRows = Window.partitionBy(df.dept).orderBy(df.salary)\
		 .rowsBetween(Window.unboundedPreceding , Window.unboundedFollowing)

df.select(df.dept,df.salary).withColumn("sum_range" , sum(df.salary).over(specRange))\
			    .withColumn("sum_row" , sum(df.salary).over(specRows))\
.show()




print('----------currentRow , unboundedFollowing--------------------')
specRange = Window.partitionBy(df.dept).orderBy(df.salary)\
		  .rangeBetween(Window.currentRow , Window.unboundedFollowing)

specRows = Window.partitionBy(df.dept).orderBy(df.salary)\
		  .rowsBetween(Window.currentRow , Window.unboundedFollowing)

df.select(df.dept,df.salary).withColumn("sum_curr_range" , sum(df.salary).over(specRange))\
			    .withColumn("sum_curr_row"  , sum(df.salary).over(specRows))\
.show()


print('---Difference------currentRow  , range(3) rows(2)-----------------------------------------')

specRange = Window.partitionBy(df.dept).orderBy(df.salary)\
		  .rangeBetween(Window.currentRow , 3)

specRow = Window.partitionBy(df.dept).orderBy(df.salary)\
		 .rowsBetween(Window.currentRow , 2)

df.select(df.dept, df.salary).withColumn("sum_range_curr_add" , sum(df.salary).over(specRange))\
			     .withColumn("sum_row_curr_add" , sum(df.salary).over(specRow))\
.show()

spark.stop()
