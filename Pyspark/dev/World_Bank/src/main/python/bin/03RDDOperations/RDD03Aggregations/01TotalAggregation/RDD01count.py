from pyspark.sql import SparkSession #type:ignore
from operator import add

spark = SparkSession.builder\
        .appName('RDD Map')\
        .master('local[1]')\
        .getOrCreate()

ord = spark.sparkContext.textFile('/practice/retail_db/orders/part-00000')
ord_items = spark.sparkContext.textFile('/practice/retail_db/order_items/part-00000')

print('--------------COUNT(Action)------------------------')
print(ord.count())

# Count the orders which are closed
print('--------------Closed Orders Count------------------')
print(f"Closed Order Count {ord.filter(lambda x : x.split(',')[3]=='CLOSED').count()} ")

print('--------------REDUCE(Action)------------------------')
# To find the total quantity which for the first 10 orders
# [1,2,3,4,5,6]
# reduce(lambda x,y: x+y) ((((1+2)+3)+4)+5)+6
total_quantity = ord_items.filter(lambda x : int(x.split(',')[1]) < 11)\
                              .map(lambda x : int(x.split(',')[3]))\
                              .reduce(lambda x,y: x+y )     
print(f"Total Quanty by using Reduce - {total_quantity}")

print('--------------REDUCE(Action)(python add function)------------------------')
# To find the total quantity which for the first 10 orders
total_quantity = ord_items.filter(lambda x : int(x.split(',')[1]) < 11)\
                              .map(lambda x : int(x.split(',')[3]))\
                              .reduce(add) 
print(f"Total Quanty by using Reduce(add) - {total_quantity}")

print('--------------REDUCE(Action)Max_Subtotal------------------------')
# for order 10 we have to calculate the max subtotal
# reduce(lambda a,b: a,b) ((((1>2)>3)>4)>5)>6 we have if else statement below
max_subtotal = ord_items.filter(lambda x : int(x.split(',')[1])==10)\
                        .reduce(lambda a,b : a if float(a.split(',')[4]) > float(b.split(',')[4]) else b)

print(f"Max Order full detail is {max_subtotal}")
print(f"Max Order is {max_subtotal.split(',')[4]}")


print('--------------REDUCE(Action)Max_Subtotal(python max or min function)------------------------')
# for order 10 we have to calculate the max subtotal
max_subtotal = ord_items.filter(lambda x : int(x.split(',')[1])==10)\
                        .map(lambda x : float(x.split(',')[4]))\
                        .reduce(max)
print(f"Max Order max is {max_subtotal}")

