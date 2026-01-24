from pyspark.sql import SparkSession  

spark = SparkSession.builder\
	.appName('Join')\
	.master('local[*]')\
	.getOrCreate()

df1 = spark.createDataFrame(data = [(1,101,'Robert'),(2,102,'Ria'),(3,103,'James')] , schema = ('empid int  , deptid int , empname string'))
df2 = spark.createDataFrame(data = [(2,102,'USA'), (4,104,'India')] , schema = 'empid int, deptid int , country string')



print('------------Multi column condition-----------------')


df1.join(other = df2 ,
		on = (df1.empid == df2.empid ) & ( df1.deptid == df2.deptid) ,
			how = 'inner').show()



df1 = spark.createDataFrame(data = [(1,101,'Robert'),(2,102,'Ria'),(3,103,'James')] , schema = ('empid int  , deptid int , empname string'))
df2 = spark.createDataFrame(data = [(2,'USA'), (4,'India')] , schema = 'empid int, country string')
df3 = spark.createDataFrame(data = [(1,'01-jan-25') , (2 , '02-feb-25'),(3 , '03-mar-25')],schema = ('empid int , joindate string'))

print('-----------join multi DF--------------')

df1.join(other = df2 ,
		on = df1.empid == df2.empid,
			how = 'inner')\
   .join(other=df3 ,
		on = df1.empid == df3.empid,
			how = 'inner').show()
