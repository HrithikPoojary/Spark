from pyspark.sql import SparkSession
from pyspark.sql.window import  Window
from pyspark.sql import window
from pyspark.sql.functions import sum,max ,min,avg,count ,avg


spark = SparkSession.builder\
        .appName('Window')\
        .master('local[*]')\
        .getOrCreate()

#print(help(window))

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


spec = Window.partitionBy(df.dept)

df.select(df.dept , df.salary).withColumn("sum" , sum(df.salary).over(spec))\
			      .withColumn("max" , max(df.salary).over(spec))\
			      .withColumn("min" , min(df.salary).over(spec))\
			      .withColumn("avg" , avg(df.salary).over(spec))\
			      .withColumn("count" , count(df.salary).over(spec))\
.show()
