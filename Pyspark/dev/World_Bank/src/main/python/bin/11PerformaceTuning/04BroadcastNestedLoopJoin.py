from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName('ShuffleHashJoin')\
        .master('local[*]')\
        .getOrCreate()


#by default spark will consider sortmerge instead of shuffle hash join
print(spark.conf.get("spark.sql.crossJoin.enabled"))

#make it True to see the broadcast nested loop join
spark.conf.set("spark.sql.crossJoin.enabled" , False)

print(spark.conf.get("spark.sql.crossJoin.enabled"))

# if small file is less that default size for broadcast join spark will pick broadcast join so we have to take bit larger file that broadcast size(10mb<)

df1 = spark.range(1,10000)
df2 = spark.range(1,100)

joined = df1.join(df2)

joined.explain()

spark.stop()
