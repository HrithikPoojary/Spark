from pyspark.sql import SparkSession

spark = SparkSession.builder\
	.appName('ShuffleHashJoin')\
	.master('local[*]')\
	.getOrCreate()


#by default spark will consider sortmerge instead of shuffle hash join
print(spark.conf.get("spark.sql.join.preferSortMergeJoin"))

#to use shuffle hash join we have manually disable the sortmerge join
spark.conf.set('spark.sql.join.preferSortMergeJoin' , False)

# if small file is less that default size for broadcast join spark will pick broadcast join so we have to take bit larger file that broadcast size(10mb<)

df1 = spark.range(1,10000000000)
df2 = spark.range(1,100000000)


joined = df1.join( df2 , "id" )

joined.explain()

spark.conf.set('spark.sql.join.preferSortMergeJoin' , True)
joined = df1.join(df2 , "id")

joined.explain()



spark.stop()
