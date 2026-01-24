from pyspark.sql import SparkSession


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

print(type(df.groupBy(df.dept)))

#print(help(df.groupBy(df.dept)))

print('---------avg------------')
df.groupBy(df.dept).avg("salary").show()


print('-------sum---------')

df.groupBy(df.dept).sum("salary").show()

print('----------max------------')
df.groupBy(df.dept).max("salary").show()

print('---------min--------')
df.groupBy(df.dept).min("salary").show()


print('------count----------')
df.groupBy(df.dept).count().show()
