from pyspark.sql import SparkSession
from pyspark.sql.functions import flatten

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()



data = [
	('Alicia',[['Java','Spark'],['Scala'],['Python']]),
	('Robert' , [[None] , ['Java'],['Hadoop']])
]
schema = ['empname' , 'arrayofarray']
emp = spark.createDataFrame(data=data,schema = schema)

emp.show(truncate = False)

print('------flatten--------')
emp.select(emp.empname , flatten(emp.arrayofarray)).show(truncate = False)



data = [
        ( 'A' , [[1] , [2]]  ),
        ( 'B' , [None , [1]] )
]
schema = ['empname' , 'arrayofarray']
emp = spark.createDataFrame(data=data,schema = schema)
emp.show()
print('-----flatten for null it will return null---------')
emp.select(flatten(emp.arrayofarray)).show(truncate = False)


data = [
        ( 'A' , [[1] , [2]]  ),
        ( 'B' , [[None] , [1]] )
]
schema = ['empname' , 'arrayofarray']
emp = spark.createDataFrame(data=data,schema = schema)
emp.show()
print('-----flatten if it is a null but inside of list it returns---------')
emp.select(flatten(emp.arrayofarray)).show(truncate = False)

#we use mutile flatten at the same time
emp.select(flatten(emp.arrayofarray),flatten(emp.arrayofarray)).show(truncate = False)



spark.stop()

