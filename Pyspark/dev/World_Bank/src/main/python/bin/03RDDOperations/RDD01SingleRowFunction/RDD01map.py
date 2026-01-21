from pyspark.sql import SparkSession #type:ignore

spark = SparkSession.builder\
        .appName('RDD Map')\
        .master('local[1]')\
        .getOrCreate()

rdd_ord = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')

rdd_ord_items = spark.sparkContext.textFile('/practice/retail_db/order_items/part-00000')

# for i in rdd_ord.take(5):
#         print(i)

#To get the order Id
print("-----------------only Order Id------------------------------")
rdd_map = rdd_ord.map(lambda x : x.split(',')[0])
for i in rdd_map.take(5):
        print(i)
print("-----------------orderid and order status-----------------------------")

#To get the order Id and Status
rdd_map = rdd_ord.map(lambda x : (x.split(',')[0], x.split(',')[3]))
for i in rdd_map.take(5):
        print(i)
print("------------------order Id + Order status------------------------------")

#To Combine order Id + Order status
rdd_map = rdd_ord.map(lambda x : x.split(',')[0] + "#" + x.split(',')[3])
for i in rdd_map.take(5):
        print(i)
print("------------------2013-07-25 00:00:00.0 = 2013/07/25------------------------------")

# To convert 2013-07-25 00:00:00.0 = 2013/07/25 
rdd_map = rdd_ord.map(lambda x : x.split(',')[1].split(' ')[0].replace('-','/'))

# To print first record
#print(rdd_ord.map(lambda x : x.split(',')[1].split(' ')[0]).first())
for i in rdd_map.take(5):
        print(i)
print("-------------------(key,value)-----------------------------")

# To convert dataset to (key,value) pair
rdd_map = rdd_ord.map(lambda x : (x.split(',')[0] , x))
for i in rdd_map.take(5):
         print(i)
print("-------------------orderId and subtotal-----------------------------")

# Order Items table get orderId and subtotal
rdd_item_map = rdd_ord_items.map(lambda x : (x.split(',')[0],x.split(',')[4]))
for i in rdd_item_map.take(5):
        print(i)
print("----------------CLOSED->closed--------------------------------")

# Convert orderstatus(CLOSED) to lower case using function

def lowerCase(string):
        return string.lower()

rdd_map = rdd_ord.map(lambda x : lowerCase(x.split(',')[3]))
for i in rdd_map.take(5):
        print(i)



