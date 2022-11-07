import math
from pyspark.sql.functions import *
data=[(1,'i love dogs'),(2,"i hate dogs and knitting"),(3,"knitting is my hobby and my passion")]
lines=sc.parallelize(data)
map1=lines.flatMap(lambda x: [((x[0],i),1) for i in x[1].split()])
reduce=map1.reduceByKey(lambda x,y:x+y)
tf=reduce.map(lambda x: (x[0][1],(x[0][0],x[1])))
map3=reduce.map(lambda x: (x[0][1],(x[0][0],x[1],1)))
map3.collect()
map4=map3.map(lambda x:(x[0],x[1][2]))
map2.collect()
reduce2=map4.reduceByKey(lambda x,y:x+y)
reduce2.collect()
idf=reduce2.map(lambda x: (x[0],math.log10(len(data)/x[1])))
idf.collect()
idf=numberofdocs_word.map(lambda x: (x[0],math.log10(len(data)/x[1])))
idf.collect()
rdd=tf.join(idf)
rdd=rdd.map(lambda x: (x[1][0][0],(x[0],x[1][0][1],x[1][1],x[1][0][1]*x[1][1]))).sortByKey()
rdd.collect()
rdd=rdd.map(lambda x: (x[0],x[1][0],x[1][1],x[1][2],x[1][3]))
rdd.toDF(["DocumentId","Token","TF","IDF","TF-IDF"]).show()

