# This saveAsSequenceFile has some brokenpipe issue

from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName("Key Aggregation")\
        .master('local[*]')\
        .getOrCreate()

rdd = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

julyOdd = rdd.filter(lambda x : x.split(',')[1].split('-')[1]== '07')

augOdd = rdd.filter(lambda x : x.split(',')[1].split('-')[1]== '08')

rddJulAug = julyOdd.union(augOdd).distinct()

# saveAsSequenceFile takes (key, value)

rddJulAugPair = rddJulAug.map(lambda x : (x.split(',')[0],x))

#requirement to create 1 file
rddJulAugPair.coalesce(1).saveAsSequenceFile('/practice/dump1/')


#Key as None
rddJulAugPair = rddJulAug.map(lambda x : (None,x))

#requirement to create 1 file
rddJulAugPair.coalesce(1).saveAsSequenceFile('/practice/dump1/')

#To load Sequence file
rddSequence = spark.sparkContext.sequenceFile('/practice/dump1')