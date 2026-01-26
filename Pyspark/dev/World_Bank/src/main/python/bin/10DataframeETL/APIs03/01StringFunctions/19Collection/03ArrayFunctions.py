from pyspark.sql  import SparkSession
from pyspark.sql.functions import col,array_max , array_min , array_distinct , array_repeat , flatten ,slice , array_position ,array_remove ,\
					array_sort,sort_array,array_contains ,array_union , array_except ,array_intersect,array_join,arrays_zip,\
					arrays_overlap,shuffle ,create_map 

spark = SparkSession.builder\
        .appName("Date")\
        .master('local[*]')\
        .getOrCreate()


data = [  ('Alicia' , 'Joseph' , ['Java' , 'Scala' , 'Spark'] , {'hair' : 'black' , 'eye':'brown'}),
          ('Robert' , 'Gee' , ['Spark' , 'Java'] , {'hair':'brown' , 'eye':None} ) ,
          ( 'Mike' ,'Bianca' , ['CSharp' , ''] , {'hair' : 'red' , 'eye' : ''}   ) ,
          ( 'John' ,  'Kumar' , None , None  ) ,
          ( 'Jeff' ,'L' , ['1','2'] , {} )
    ]

schema = ['firstname' , 'lastname' , 'langueage' , 'properties']

emp1 = spark.createDataFrame(data = data , schema = schema)

#emp1.show(truncate = False)


emp2 = spark.createDataFrame(data = [('Robert' , 35 ,40,40 ),
                                     ('Ram'    , 31 ,33,29 ),
                                     ('John'   , 95,89 ,91) ] \
                                        , schema =['name' , 'score1' , 'score2' , 'score3'])
#emp2.show()


emp3 = spark.createDataFrame(data = [  ('John' , [10,20,20] , [25,11,10]) ,
                                       ('Robert',[15,13,55],[5,None ,13]) ,
                                        ('James',[11,13,45],[5,89,79] )  ] , schema = ['empname' , 'score_arr1' , 'score_arr2'])

#emp3.show()

df = spark.sql("select array(struct(1,'a') , struct(2,'b')) as data")

print('----------array max and min-------------')

emp3.select(emp3.score_arr1 , emp3.score_arr2 , array_max(emp3.score_arr1).alias("arr1_Max") , array_min(emp3.score_arr2).alias("arr2_Min")).show()

print('-------array distinct------------')
emp3.select(emp3.score_arr1 , array_distinct(emp3.score_arr1)).show()


#repeat =3 * [2]  == [[2],[2],[2]]
print('-------repeat--------------')
demo = emp3.select(emp3.score_arr1, array_repeat(emp3.score_arr1,3).alias('repeated_array'))
demo.show(truncate = False)

#flatten = [[2],[2],[2]] = [2,2,2]
#array_distinct = [2,2,2] = [2]

demo.select(array_distinct(flatten(demo.repeated_array))).show(truncate =False)


print('---------slice--------------')
#slice(col,start ,length)
#[1,2,3,4,5] slice(col ,2,2)  -> [2,3]
# 1 2->start
#   1,2->length
emp1.select(emp1.langueage , slice(emp1.langueage , 2 , 2)).show()


#array_position first occurance of the given value
#starts with index 1
#array_position(col , value)
print('--------array_position-----------')
emp3.select(emp3.score_arr1 , array_position(emp3.score_arr1 , 20)).show()

#array_remove(col , element)
print('-------array_remove----------')
emp3.select(emp3.score_arr1 , array_remove(emp3.score_arr1 , 13)).show()

#array_sort(col)
print('-------array_sort----------')
emp3.select(emp3.score_arr1 , array_sort(emp3.score_arr1).alias('sorted')).show()


#sort_array(col , asc=True)  # asc - nulls first desc - nulls last
print('-----sort_array-------------')
emp3.select(emp3.score_arr2 ,sort_array(emp3.score_arr2 , asc = True).alias("Asc_null_fist"),
			     sort_array(emp3.score_arr2 , asc = False).alias('desc_null_last'))\
.show()

print('---------array_contains--------------')
#array_contains(col,value)
emp1.select(emp1.langueage , array_contains(emp1.langueage , 'Scala').alias("contains")).show()

print('---------array_union---------------')
#array_union(co1,col2)
emp3.select(emp3.score_arr1 , emp3.score_arr2 , array_union(emp3.score_arr1 , emp3.score_arr2).alias("Union_wo_duplicates")).show(truncate= False)

#array_except(col1 , col2)
#like minus
print('---------array_except------------------')
emp3.select(emp3.score_arr1 , emp3.score_arr2 , array_except(emp3.score_arr1 , emp3.score_arr2).alias("except")).show()

print('-----------array_intersect----------------------')
#array_intersect(co1,col2)
emp3.select(emp3.score_arr1 , emp3.score_arr2 , array_intersect(emp3.score_arr1 , emp3.score_arr2).alias("intersect")).show()

print('--------array_join---------------')
#array_join(col , delimiter , null_replacement = None)
emp3.select(emp3.score_arr2 , array_join(emp3.score_arr2 , '#' , '-*-').alias('array_join')).show()


#arrays_zip(*col)
#[1,2]   ['a','b']  ==>[[1,'a'] , [2,'b']]
#[3,4]   ['c','d']  ==>[[3,'c'] , [4,'d']]

print('--------arrays_zip--------------')
emp3.select(emp3.score_arr1 , emp3.score_arr2 , arrays_zip(emp3.score_arr1 , emp3.score_arr2)).show(truncate= False)

print('-------arrays_overlap--------------')
#arrays_overlap
#[1,2]    [2,3] - > true( 2 overlap)
#[2,null] [3,4] - > null(null value and no match, if no overlap that is false)
emp3.select(emp3.score_arr1 , emp3.score_arr2 , arrays_overlap(emp3.score_arr1 , emp3.score_arr2)).show(truncate= False)


#shuffle - random  - costlier operation
print('--------shuffle--------------')
emp3.select(emp3.score_arr2 ,shuffle(emp3.score_arr2)).show(truncate= False)



spark.stop()






