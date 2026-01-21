from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder\
	.appName('SQL')\
	.master('local[2]')\
	.getOrCreate()

lst1 = [("Luffy" ,19),("Zoro" , 21)]
lst2 = [("Luffy",101),("Zoro" , 102)]

df = spark.createDataFrame(data = lst1 , schema = ['Name','Age'])

# print(help(spark.sql))
# TempView Only acive for one session
# using dataframe will insert data data into table
df.createOrReplaceTempView("Pirates")

# This will give df 
spark.sql(''' select * from Pirates where age = 21''').show()


#GlobalTempView is active for all session. 

df = spark.createDataFrame(data= lst2 , schema = ['Name', 'DeptNo'])

df.createOrReplaceGlobalTempView('Department')

# global view will be saved in the global_temp schema
spark.sql(''' select * from global_temp.Department where deptno = 101''').show()
