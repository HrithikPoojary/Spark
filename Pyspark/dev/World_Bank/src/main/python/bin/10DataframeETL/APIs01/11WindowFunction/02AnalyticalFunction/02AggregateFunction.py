from pyspark.sql import SparkSession
from pyspark.sql.window import  Window
from pyspark.sql import window
from pyspark.sql.functions import sum,row_number,col,first,last


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
specOrder = Window.partitionBy(df.dept).orderBy(df.salary.desc())

df.select(df.dept,df.salary).withColumn("sum_no_Order" , sum(df.salary).over(spec))\
			   .withColumn("sum_order" , sum(df.salary).over(specOrder))\
			   .withColumn("first_High_salary" , first(df.salary).over(specOrder))\
			   .withColumn("last_low_salary" , last(df.salary).over(specOrder))\
.show()



# To hundle duplicates sum
df.select(df.dept,df.salary).withColumn("sum_no_Order" , sum(df.salary).over(spec))\
                           .withColumn("row_number" , row_number().over(Window.partitionBy(df.dept).orderBy(df.salary.asc() )) )\
			    .withColumn('sum_order' ,sum(df.salary).over(Window.partitionBy(df.dept).orderBy(col("row_number")) ))\
			   .select(col("dept") , col("salary") , col("sum_no_Order"), col("sum_order") )\
.show()


spark.stop()














