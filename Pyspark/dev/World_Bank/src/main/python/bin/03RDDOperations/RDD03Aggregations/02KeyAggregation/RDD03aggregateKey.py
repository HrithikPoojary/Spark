from pyspark.sql import SparkSession #type:ignore
from operator import add

spark = SparkSession.builder\
        .appName("Key Aggregation")\
        .master('local[*]')\
        .getOrCreate()

rddItems = spark.sparkContext.parallelize(
        [(2,'Joseph',200),(2,'Jimmy',250),(2,'Tina',130),(4,'Jimmy',50),(4,'Tina',300)
         ,(4,'Joseph',150),(4,'Ram',200),(7,'Tina',200),(7,'Joseph',300),(7,'Jimmy',80)] ,2
)

rddpair = rddItems.map(lambda x : (x[0],(x[1],x[2])))
print('----------------RDD Items---------------------')
for i in rddItems.take(5):
        print(i)

print('----------------RDD Pair---------------------')
for i in rddpair.take(5):
        print(i)

# aggregateByKey(zero_val , squenceOperator,combinerOperator)
# zero_val = initialize the accumalator
# squenceOperator(accumalator,values (from (key,values) the key will be automatically picked)(local process)
#       each tile accumalator will be stored with new value
# combinerOperator(acculator1,accumalator2)(one accumalator from each partition based on the key)  

zero_val1 = 0 

def sequenceOperator1(acc,element):
        if (acc > element[1]):
                return acc
        else:
                return element[1]

def combinerOperator1(acc1,acc2):
        if (acc1 > acc2) :
                return acc1
        else:
                return acc2

rdd_agg = rddpair.aggregateByKey(zero_val1,sequenceOperator1,combinerOperator1)

print('----------------RDD Aggregated Result-----------------')
for i in rdd_agg.collect():
        print(i)

zero_val2 = ('',0)

def squenceOperator2(acc,element):
        if acc[1] > element[1]:
                return acc
        else :
                return element
        
def combinerOperator2(acc1,acc2):
        if acc1[1] > acc2[1]:
                return acc1
        else :
                return acc2
        
rdd_agg = rddpair.aggregateByKey(zero_val2,squenceOperator2,combinerOperator2)
print('----------------RDD Aggregated USING Tuple-----------------')
for i in rdd_agg.collect():
        print(i)


zero_val3 = (0,0)

def sequenceOperator3(acc,element):
    return ( acc[0] + element[1] , acc[1] + 1 )

def combinerOperator3(acc1,acc2):
        return (acc1[0]+acc2[0] , acc1[1]+acc2[1])

rdd_agg = rddpair.aggregateByKey(zero_val3,sequenceOperator3,combinerOperator3)

print('---------------Example 3 Important------------------')
for i in rdd_agg.collect():
        print(i)

