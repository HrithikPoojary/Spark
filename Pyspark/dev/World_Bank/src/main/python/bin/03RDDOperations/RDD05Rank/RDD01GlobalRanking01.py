# Using sortByKey + take

from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('Rank')\
        .master('local[*]')\
        .getOrCreate()

rddProd = spark.sparkContext.textFile('/practice/retail_db/products/part-00000')

print('-----------Print Product Records---------------')
for i in rddProd.take(5):
        print(i)

print('----------No built in APIs in RDD for Rank instead we can use sortByKey and take------')

rddKeys = rddProd.map(lambda x: (float(x.split(',')[4]),x))
for i in rddKeys.take(5):
        print(i)

# rddSorted = rddKeys.sortByKey(ascending = True)
# print('----------Bad Data------')
# for i in rddSorted.take(5):
#         print(i)
print('------------Bad Data in product file--------------------------')
print(rddProd.filter(lambda x : x.split(',')[4]=='').count())

rddFilteredData = rddProd.filter(lambda x : x.split(',')[4]!='')\
                        .map(lambda x : (float(x.split(',')[4]),x))

rddSort = rddFilteredData.sortByKey(ascending=False)
print('------------Descending the price to get highest value---------------')
for i in rddSort.take(5):
        print(i)

print('---------Save top 10 Ouptput----------')

rddTop10 = rddSort.take(10)

for i in rddTop10:
        print(i)

