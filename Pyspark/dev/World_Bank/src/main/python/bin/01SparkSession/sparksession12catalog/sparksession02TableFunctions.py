from pyspark.sql import SparkSession   #type:ignore

spark = SparkSession.builder\
			.appName('TableFunctions')\
			  .master('local[2]')\
				.getOrCreate()

df = spark.read.load(path = '/practice/retail_db/orders_parquet', format = 'parquet')


# df.write.mode("overwrite").saveAsTable("default.orders")
# To create MANAGED table
# spark.sql(""" create table students(id int)""")
# spark.sql(""" select count(*) from students""")
# spark.catalog.listColumns(tableName = 'students' , dbName = 'default')

df.createOrReplaceTempView('orders')

print('-------------Existing tables---------------\n')
print(spark.catalog.listTables())
print('orders' in [i.name for i in spark.catalog.listTables()])
print([i.name for i in spark.catalog.listTables() if i.tableType == 'TEMPORARY'])


print('--------------Column for the specific table----------------------\n')
print(spark.catalog.listColumns(tableName = 'orders',dbName = None))

print('---------------cacheTable for better performance----------------------------')
spark.catalog.cacheTable(tableName = 'orders')
print(spark.catalog.isCached(tableName = 'orders'))


spark.catalog.uncacheTable(tableName = 'orders')
print(spark.catalog.isCached('orders'))

print('---------clearCache in current session remove all memory cache that means cache memory----------------------')

spark.catalog.clearCache()

print('----------dropTempView----------')

spark.createDataFrame([(1,1)]).createOrReplaceTempView('my_table')

print( [ i.name for i in spark.catalog.listTables() if i.tableType == 'TEMPORARY']  )
spark.catalog.dropTempView(viewName = 'my_table')


print( [ i.name for i in spark.catalog.listTables() if i.tableType == 'TEMPORARY']  )

print('----------------To check the functions----------------------------')

#print(spark.catalog.listFunctions())

print( 'initCap' in [ i.name for i in spark.catalog.listFunctions()] )

print(spark.catalog.listFunctions()[0])


spark.stop()
