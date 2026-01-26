from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder\
	.appName('Sampling')\
	.master('local[2]')\
	.getOrCreate()

df = spark.range(100)

print(df.count())
#sample(withReplacement = (same can be pick twice) , fraction = (0 - 1 percentage of the records pick) , seed = (same record can be get for multiple calls))
#withReplacement = False
df.sample(fraction = 0.2).show()

df.sample(withReplacement = True  , fraction = 0.1 , seed =30).show()
df.sample(withReplacement = True  , fraction = 0.1 ).show()
df.sample(withReplacement = True  , fraction = 0.1 , seed =30).show()

#sampleBy  (col , fractions , seed)
# col - simple column
# fransactions - it like level shown below if you mention any level that is consider as a zero in below ex 2 is zero



df = spark.range(100).select( (col("id") % 3 ).alias("key") )
df.distinct().show()

sampled = df.sampleBy("key" , fractions={0:0.1 , 1: 0.2} , seed  = 10 )
sampled.show()

sampled.groupBy(sampled.key).count().orderBy(sampled.key).show()





spark.stop()
