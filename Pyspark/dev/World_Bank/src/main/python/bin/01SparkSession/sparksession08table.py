from pyspark.sql import SparkSession  #type:ignore

spark = SparkSession.builder\
	.appName('SQL')\
	.master('local[2]')\
	.getOrCreate()

#print(help(spark.table))

lst1 = [("Luffy" , 19) , ("Zoro" , 21)]

df = spark.createDataFrame(data=lst1,schema = ['Name string','Age long'])

df.createOrReplaceTempView('pirates')

df_emp = spark.table('pirates') # this will return dataframe

print(sorted(df.collect()) == sorted(df_emp.collect()) )

