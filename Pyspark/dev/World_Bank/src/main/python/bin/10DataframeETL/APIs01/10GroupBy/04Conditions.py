from pyspark.sql import SparkSession
from pyspark.sql.functions import max,min,avg,col

spark = SparkSession.builder\
        .appName('groupBy')\
        .master('local[*]')\
        .getOrCreate()


data = [
    ('James' , 'Sales','NY' , 9000 , 34),
    ('Alicia' , 'Sales','NY' , 8600 , 56),
    ('Robert' , 'Sales','CA' , 8100 , 30),
    ('Lisa' , 'Finance','CA' , 9000 , 24),
    ('Deja' , 'Finance','CA' , 9900 , 40),
    ('Sugie' , 'Finance','NY' , 8300 , 36),
    ('Ram' , 'Finance','NY' , 7900 , 53),
    ('Kyle' , 'Marketing','CA' , 8000 , 25),
    ('Reid' , 'Marketing','NY' , 9100 , 50)
]

schema = ['empname', 'dept','state','salary','age']

df = spark.createDataFrame(data = data ,schema = schema)


# error ->df.groupBy(df.dept).agg(max("salary"), min("salary") , avg("salary")).where(df.state =='NY').show()
# after group by we can access only  grouby columns and aggregated columns
#before groupby we can use df all columns

df.where(df.state =='NY').groupBy(df.dept).agg(max("salary"),min("salary") , avg("salary")).show()

#col - spark explicitly ask to cosider as a column
df.groupBy(df.dept).agg(max("salary").alias('max_salary'),
			min("salary").alias('min_salary'),
			avg("salary").alias('avg_salary') )\
			.where(col("min_salary") > 8001)\
.show()
