from os.path import realpath
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("damu1000")
conf.set("spark.executor.memory", "2g")
conf.set("spark.hadoop.validateOutputSpecs", "false")

sc = SparkContext(conf=conf)
lines = sc.textFile(".//data//TrainingSet.csv")


lines = lines.map(lambda x: x.split(","))	#splitting into 2D array 

i=1
data = lines.map(lambda x: (x[37], x[38], x[39], i+1971, x[i]))	#arranging by country, series code, series name, year, data


for i in range(2,37):
	data = data.union(lines.map(lambda x: (x[37], x[38], x[39], i+1971, x[i])) )	#arranging by country, series code, series name, year, data


#lines = lines.take(5)
print data.take(5)

data.saveAsPickleFile("result")

#print line[0]
#print line[1][38]

