#import findspark
import os
#findspark.init()
from audioop import add
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    SPARK_MASTER='local[2]'
    SPARK_APPLICATION_NAME='Streamer'
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    #sc = SparkContext(SPARK_MASTER,SPARK_APPLICATION_NAME)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    fig = plt.figure()
    print("ADBI")
    print(counts)
    pcounts = [x[0][1] for x in counts]
    ncounts = [x[1][1] for x in counts]
    timestamp = range(0,len(counts))
    plt.plot(timestamp,pcounts,"bo-",label='positive')
    plt.plot(timestamp,ncounts,"go-",label='negative')
    plt.legend(loc='upper left')
    plt.ylabel('Word count')
    plt.xlabel('Time step')
    fig.savefig("plot.png")

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    word_list = set(open(filename).read().splitlines())
    return word_list
    

def findCount(pnlist,x):
	return len(pnlist.intersection(set(x.lower().split(" "))))

def updateFunction(newVal,runningCounts):
    if runningCounts is None:
        runningCounts = 0
    return sum(runningCounts,newVal)

def stream(ssc, pwords, nwords, duration):
    print("hello from the other svide")
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    kstream.pprint()

    modified_tweets=tweets.flatMap(lambda x:[("positive",findCount(pwords,x)),("negative",findCount(nwords,x))])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE

    modified_tweets.pprint()
    # for row in modified_tweets:
    #     print(row[0] + "," +str(row[1]))
    pos_nefg_words = modified_tweets.reduceByKey(add)
    pos_nefg_words.pprint()
    runningCounts = pos_nefg_words.updateStateByKey(updateFunction)
    runningCounts.pprint()
    print("hello from the other side")
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negacleartive", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    pos_nefg_words.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-assembly_2.10-1.6.0.jar pyspark-shell'
    main()
#$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 /Users/pranavsingaraju/Desktop/CSC591-ADBI/sentiment/twitterStream.pyIvy Default Cache set to: /Users/pranavsingaraju/.ivy2/cache
