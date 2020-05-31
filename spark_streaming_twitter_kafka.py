from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import time
import datetime
import sys
from pyspark.streaming.kafka import KafkaUtils
from math import log


KAFKA_BROKER="172.31.39.113:9092"
#KAFKA_BROKER =
KAFKA_TOPIC="twitter_topic"

###########################################################################################################
# The following code has solutions to bonus questions:
# 1. Own twitter credentials
# 2. Computation of Approximation of the total no. of elemnts using HyperLogLog (Since it was written that we can make use of any
#    approximate algorithm, I found it better to go for HyperLogLog because its more efficient than Flajolet-Martin)
# 3. COnnects with Kafka to pulls the tweets (Please note that theere are code changes in twitter_app.py as well because twitter_app
#    should send the data to Kafka and not act as a TCP server; so twitterApp would work as a Kafka Producer). The Producer file
#    is also attached - twitter_app_bonus.py
#
# Important: to run this code we make use of a jar too. The command should be following:
#            spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar spark_streaming.py
##############################################################################################################


######################################################################################
# HyperLogLog Implementation
######################################################################################
class HLL(object):
    P32 = 2 ** 32

    def __init__(self, p=14):
        self.p, self.m, self.r = p, 1 << p, [0] * (1 << p)

    def add(self, x):
        x = hash(x)
        i = x & HLL.P32 - 1 >> 32 - self.p
        z = 35 - len(bin(HLL.P32 - 1 & x << self.p | 1 << self.p - 1))
        self.r[i] = max(self.r[i], z)

    def count(self):
        a = ({16: 0.673, 32: 0.697, 64: 0.709}[self.m]
             if self.m <= 64 else 0.7213 / (1 + 1.079 / self.m))
        e = a * self.m * self.m / sum(1.0 / (1 << x) for x in self.r)
        if e <= self.m * 2.5:
            z = len([r for r in self.r if not r])
            return int(self.m * log(float(self.m) / z) if z else e)
        return int(e if e < HLL.P32 / 30 else -HLL.P32 * log(1 - e / HLL.P32))

######################################################################################

#approximate no. of unique elements in a dstream and
# comparison
# by putting the elemnts in a set data structure
def get_unique_acc_to_HLL(rdd):
    list_of_words = []

    # for verifying if the HLL gives correct results
    set_of_words = set()

    # Create an instance of HyperLogLog
    h = HLL()
    tuple_of_words = rdd.collect()
    for x in tuple_of_words:
        list_of_words.append(x)
        h.add(x)
        set_of_words.add(x)
    print("*******************BEGIN HLL stats****************************")
    print ("No. of words:", len(list_of_words))
    print("Unique words - HyperLogLog:", h.count())
    print("Num words in set", len(set_of_words))
    print("*******************END HLL stats****************************")


def get_curr_timestamp():
    import time
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M:%S')
    return st

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(word=w[0], word_count=w[1]))
        # create a DF from the Row RDD
        word_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        word_df.registerTempTable("words")
        # get the top 10 words from the table using SQL and print them
        word_counts_df = sql_context.sql("select word, word_count from words order by word_count desc limit 10")
        word_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# create spark configuration
conf = SparkConf()
conf.setAppName("Maz_twitter_streaming_app")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpointTwitterApp")
# Connect to Kafka topic # KAFKA_TOPIC and KAFKA_BROKER are defined at the top of the file
dataStream = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC],{"metadata.broker.list": KAFKA_BROKER})
lines = dataStream.map(lambda x: x[1])
# split each tweet into words
words = lines.flatMap(lambda line: line.split(" "))

# Get unique elements according to HyperLogLog approximation and print on the screen
words.foreachRDD(get_unique_acc_to_HLL)

wordmap = words.map(lambda x: (x, 1))
# adding the count of each hashtag to its last count
word_totals = wordmap.updateStateByKey(aggregate_tags_count)
word_totals.pprint()

# do processing for each RDD generated in each interval
word_totals.foreachRDD(process_rdd)

# Publish the results in HDFS folder - uncomment if you want..
#word_totals.saveAsTextFiles(/users/marang.mutloatse/)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
