from __future__ import print_function
from pymongo import MongoClient

from pyspark import SparkConf,SparkContext
from pyspark.sql import Row,SQLContext

import json
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
#        spark = getSparkSessionInstance(rdd.context.getConf())
#
#        # Convert RDD[String] to RDD[Row] to DataFrame
#        rowRdd = rdd.map(lambda w: Row(word=w))
#        wordsDataFrame = spark.createDataFrame(rowRdd)
#        wordsDataFrame.show()
#        # Creates a temporary view using the DataFrame
#        wordsDataFrame.createOrReplaceTempView("words")
#
#        # Do word count on table using SQL and print it
#        wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
#        wordCountsDataFrame.show()
        temp = rdd.collect()
        new_list = []
        word_list = [i[0] for i in temp]
        for i in word_list:
            if ("com" in i):
                new_list.append(i)
        #print("printing word list")
        #print(new_list)
        my_dict = {}
        #mongo connection information
        client = MongoClient('127.0.0.1', 27017)
        print("mongoClient: " + str(client))
        db = client['ie']
        print("db: " + str(db))
        collectionNm = 'links'
        collection = db[collectionNm]
        print("Adding links to collection: " + str(collection))        
        for i in new_list:
            my_dict.update({"url":i})
            print("Inserting link to Mongo ")
            print(i)
            if (collection.find({"url":i}).count() == 0):
                collection.insert_one(my_dict)
            my_dict.clear()
        client.close()
    except:
        pass

# Create a local StreamingContext with two working threads and batch interval of 5 seconds
sc = SparkContext("local[2]", "NetworkWordCount")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)

# We need to create the checkpoint
ssc.checkpoint("my_checkpoint")

# Create a DStream that will connect to hostname:port, like localhost:10002
lines = ssc.socketTextStream("localhost", 10000)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))

#wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts = pairs.updateStateByKey(updateFunc)

#Establish a Mongo connection



wordCounts.foreachRDD(process)
#word_list = [i[0] for i in wordCounts]
#
#diff = len(wordCounts)-(len(word_list))
#
#if (diff > 0):
#    i = 0
#    len_old = len(word_list)
#    len_latest = len(wordCounts)
#    while (i <= diff):
#        word_list.append(wordCounts[len_old+i])
#        i += 1
#        print("Inside the loop")

#
# Print the first ten elements of each RDD generated in this DStream to the console
#wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
