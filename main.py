import os
import sys
from random import random

#Done to configure spark. Remove if you are able to import SparkContext from pyspark
def configure_spark(spark_home=None, pyspark_python=None):
    spark_home = spark_home
    os.environ['SPARK_HOME'] = spark_home

    # Add the PySpark directories to the Python path:
    sys.path.insert(1, os.path.join(spark_home, 'python'))
    sys.path.insert(1, os.path.join(spark_home, 'python', 'pyspark'))
    sys.path.insert(1, os.path.join(spark_home, 'python', 'build'))

    # If PySpark isn't specified, use currently running Python binary:
    pyspark_python = pyspark_python or sys.executable
    os.environ['PYSPARK_PYTHON'] = pyspark_python

#configure_spark('C:\spark\spark-2.2.0-bin-hadoop2.7\spark-2.2.0-bin-hadoop2.7')
#os.environ['SPARK_HOME'] = 'C:\spark\spark-2.2.0-bin-hadoop2.7\spark-2.2.0-bin-hadoop2.7'
#sys.path.append('C:\spark\spark-2.2.0-bin-hadoop2.7\spark-2.2.0-bin-hadoop2.7bin')

from pyspark import  SparkContext


data = [(1, "The horse raced past the barn fell"),
		(2, "The complex houses married and single soldiers and their families"),
		(3, "There is nothing either good or bad, but thinking makes it so"),
		(4, "I burn, I pine, I perish"),
		(5, "Come what come may, time and the hour runs through the roughest day"),
		(6, "Be a yardstick of quality."),
		(7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
		(8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
		(9, "The car raced past the finish line just in time."),
		(10, "Car engines purred and the tires burned.")]

#get spark context
sc=SparkContext.getOrCreate()

dataRDD = sc.parallelize(data)
#Get only the lines from data
dataLines= dataRDD.map(lambda x:x[1])
#split words with respect to ' ' and map all the words as keys with 1 as value and then reduce all the keys to get count of the word
wordsCount = dataLines.flatMap(lambda x:x.split(' ')).map(lambda x:(x.lower(),1)).reduceByKey(lambda x,y:x+y).collect()

data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
			('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
data2 = [('R', [x for x in range(50) if random() > 0.5]),('S', [x for x in range(50) if random() > 0.75])]
dataRDD1 = sc.parallelize(data1)
dataRDD2 = sc.parallelize(data2)

#Map the elements in the set as keys and the set name as values.
#Reduce the elements by keys to get a tuple of sets it belongs to.
#Filter out elements if tuple has S as value
#Map the elements back with set name as key and element name as values
#Reduce the elements by key to get (set name, list of values) as a tuple
setDifference1 = dataRDD1.flatMap(lambda z:((y,z[0]) for y in z[1])).reduceByKey(lambda x,y:(x,y)).filter(lambda x:'R' in x[1] and 'S' not in x[1]).map(lambda x:(x[1],[x[0]])).reduceByKey(lambda x,y:x+y).collect()
setDifference2 = dataRDD2.flatMap(lambda z:((y,z[0]) for y in z[1])).reduceByKey(lambda x,y:(x,y)).filter(lambda x:'R' in x[1] and 'S' not in x[1]).map(lambda x:(x[1],[x[0]])).reduceByKey(lambda x,y:x+y).collect()

print(wordsCount)
print(setDifference1)
print(setDifference2)

