from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

def main(port):
    conf = SparkConf().setAppName("netcount")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)

    word_counts = ssc.socketTextStream("localhost", port) \
                     .flatMap(lambda line: line.split(" ")) \
                     .map(lambda x: (x, 1)) \
                     .reduceByKey(lambda x, y: x + y)

    word_counts.pprint()

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print "Must specify port to listen to."
        sys.exit(1)

    main(int(sys.argv[1]))
