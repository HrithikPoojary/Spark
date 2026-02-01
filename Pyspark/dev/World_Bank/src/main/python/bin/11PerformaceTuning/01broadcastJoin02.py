from pyspark.sql import SparkSession
from pyspark.sql.functions import  broadcast


spark = SparkSession.builder\
        .appName('PerformanceTuning')\
        .master('local[*]')\
        .getOrCreate()


largedf = spark.range(1,1000000000)
smalldata = [(1,'a') , (2,'b') , (3 , 'c')]

schema = ['id' , 'col2']

smalldf = spark.createDataFrame(data = smalldata , schema = schema)

# In theory optimizer can figure out the sizes of both large data frame and small data frame.It can apply the broadcast join.
#but a small data frame created on top of a local collection,spark has no ways of knowing what is the size of this small data frame , so it may not use >
#the broadcast join


'''
This will not apply broadcast join
joined = largedf.join(smalldf,"id")
joined.explain()

'''

joined = largedf.join(broadcast(smalldf) , "id")

joined.explain()

