from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType, TimestampType
from pyspark.sql.functions import lower , substring , col

spark = SparkSession.builder\
        .appName('ETL')\
        .master('local[*]')\
        .getOrCreate()


df1 = spark.createDataFrame(data = [(1,'Robert') ,(2,'Ria') , (3,'James')] , schema = ('empid int , empname string'))
df2 = spark.createDataFrame(data = [(2 , 'USA') , (4,'India')],schema = ('empid int , country string'))


# join(other  , on , how = default (inner))

print('---------inner------------')
df1.join(other = df2 , on = df1.empid == df2.empid , how = 'inner').select(df1.empid,df1.empname,df2.country).show()


print('----------left-----------')

df1.join(other=df2 , on = df1.empid == df2.empid , how = 'left' ).select(df1.empid,df1.empname , df2.country).show()

print('----------right----------')

df1.join(other = df2 , on = df1.empid == df2.empid , how = 'right').select(df1.empid,df1.empname , df2.country).show()

print('---------full(outer)-------------')

df1.join(other = df2 , on = df1.empid==df2.empid , how = 'full').select(df1.empid , df1.empname , df2.country).show()

print('-------cross(axb)------------------')
# on = we can give any column df1==df1
df1.join(other=df2 , on = df1.empid == df1.empid ,how = 'cross').show()

print('----------crossJoin----------')
# cross == crossJoin api
df1.crossJoin(df2).show()



print('----------leftanti--------------')
#leftanti -> df1 not matching df2
#leftanti is faster than normal joins recommanded to use if no need of df2 columns
#also we connot access columns of df2 it is only checking df1
df1.join(other = df2 , on = df1.empid == df2.empid , how = 'leftanti').show()

# error - > df1.join(other = df2 , on = df1.empid == df2.empid , how = 'leftanti').select(df2.country).show()

print('----------leftsemi-----------')

# leftsemi is similar to the  inner join only difference is  we cannot access df2 columns
# leftsemi is faster than leftsemi  if neccessary we should use this

df1.join(other = df2 , on = df1.empid == df2.empid , how = 'leftsemi').show()
# error - > df1.join(df2 , df1.empid == df2.empid , 'leftsemi').select(df2.country).show()



df = spark.createDataFrame(data = [(1,'Robert' ,2 ) ,(2,'Ria' , 3 ) , ( 3 , 'James' , 5) ] , schema = ('empid int , empname string , managerid int'))


df.show()
#col function is used to covert  into column
print('--------selfJoin------------')
df.alias("emp1").join( other = df.alias("emp2") ,
			on = col("emp1.managerid") == col("emp2.empid") ,
			  how = 'inner')\
		.select(col("emp1.empid") , col("emp1.empname") , col("emp1.empid").alias("managerid") , col("emp2.empname").alias("managername"))\
		.show()






spark.stop()








