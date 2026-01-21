# This below flow will create 670.3MB file
df = spark.range(100000) #type:ignore
df = df.select(df.id,df.id*2,df.id*3)


df = df.union(df)
df = df.union(df)
df = df.union(df)
df = df.union(df)
df = df.union(df)

RDD = df.rdd.map(lambda x : str(x[0]) + ',' + str(x[1]) + ',' + str(x[2]))
RDD.saveAsTextFile('/practice/test/sample')