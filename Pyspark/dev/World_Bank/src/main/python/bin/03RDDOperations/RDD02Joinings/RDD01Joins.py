from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Map')\
        .master('local[1]')\
        .getOrCreate()

rdd_ord = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')
rdd_ordItem = spark.sparkContext.textFile('/practice/retail_db/order_items/part-00000')

# To find the subtotal for each customer id
# Customer_id in rdd_ord rdd 3 rd column
# subtotal in rdd_ordItem rdd 6 th column
# To solve this we have to convert the rdds to key-value pairs
#    inner join
# rdd_ord(k,v)  rdd_ordItem(k,w)  join with k = (k,(v,w))
rdd_map_ord = rdd_ord.map(lambda x : (x.split(',')[0],x.split(',')[2]))
rdd_map_orditem = rdd_ordItem.map(lambda x : (x.split(',')[0],x.split(',')[5]))
innerJoinCust = rdd_map_ord.join(rdd_map_orditem)
print('---------------INNER JOIN-----------------------------')
for i in innerJoinCust.take(5):
        print(i)
print('---------------Only Get CustomerId and subtotal---------------------')
for i in innerJoinCust.map(lambda x : x[1]).take(5):
        print(i)
print('---------------To customize CustomerId and subtotal---------------------')
for i in innerJoinCust.map(lambda x : x[1][0]+ ' -> '+x[1][1]).take(5):
        print(i)

#   Left Join 
#  (k,(v,w))
#  (k, (v, None))
innerJoinCust = rdd_map_ord.leftOuterJoin(rdd_map_orditem)
print('---------------LEFT JOIN---------------------')
for i in innerJoinCust.take(5):
        print(i)

#   Right Join 
#  (k,(v,w))
#  (k, (None, v))
innerJoinCust = rdd_map_ord.rightOuterJoin(rdd_map_orditem)
print('---------------RIGHT JOIN-----------------------------')
for i in innerJoinCust.take(5):
        print(i)

#   Outer Join 
#  (k,(v,w))
#  (k, (v, None))
#  (k, (None, v))
#  (k, (Optional[v], Optional[w]))
innerJoinCust = rdd_map_ord.fullOuterJoin(rdd_map_orditem)
print('---------------OUTER JOIN-----------------------------')
for i in innerJoinCust.take(5):
        print(i)







