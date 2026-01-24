from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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

#pivot converting rows into columns

df1 = df.select(df.dept,df.state,df.salary)

df1 = df1.groupBy(df.dept,df.state).agg( sum("salary").alias("salary"))

df1.show()

df1 = df1.groupBy(df1.dept).pivot("state").sum("salary")

# to unpivot we don't have any function in spark but we can selectExpr sql stack operators
spark.sql(""" select stack(3 ,1,2,3,4,5,6)""").show()


df1.createOrReplaceTempView("tab")

spark.sql("select * from tab").show()

#CA , NY - columns
#'ca','ny' - hardcoded value
spark.sql(""" select dept , stack (2 , 'ca' , CA , 'ny' , NY) as (state , salary) from tab""").show()


df1.selectExpr("dept" , "stack(2,'ca', CA , 'ny' , NY) as (state , sakary)").show()
spark.stop()
