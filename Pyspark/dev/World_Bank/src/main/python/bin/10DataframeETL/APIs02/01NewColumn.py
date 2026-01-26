from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number,rank,dense_rank,percent_rank,ntile,cume_dist
from pyspark.sql.functions import monotonically_increasing_id ,lit,concat ,expr

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

# monotonicall_increasing_id - >  generates unique and increase id but not consecutive,  64 -bit integer we can use as primary key or unique key
print('---------monotonically_increasing_id--------------------')
df.withColumn("id" , monotonically_increasing_id()).show()

# lit - to create static column,give a value to the all rows like select 'good' from employess.

print('-------------lit-----------------------')
df.withColumn("country" , lit("USA")).show()

print('---------lit using concat------------------')
df.withColumn("deptState" , concat(df.dept , lit('##') , df.state)).show(5)
df.select(concat(df.state , lit(' * ') , df.dept)).show(5)
df.withColumn('multiple_age' , lit(df.age * 2)).show()



#expr - sql like syntax
#expr - we can sql functions like case when , concat oprator like ||
#expr - spark does't compile this script it will check runtime
print('-------------expr-------------------')
df.withColumn('empname_len' , expr("length(empname)")).show()

df.withColumn("Age_des" , expr("case when age > 50 then 'SENIOR' else 'JUNIOR'  end")).show(5)
df.withColumn('emp_state' , expr(" empname || ' - ' || state ")).show(5)
df.select(expr(" empname || ' - ' || state as Combine")).show(2)
df.withColumn("multiple_age"  , expr( " age * 2 " )).show()



spark.stop()
