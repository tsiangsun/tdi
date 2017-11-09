from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

def main():
    conf = SparkConf().setAppName("netcount")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)

    rdds = [sc.parallelize([i]) for i in xrange(100)]

    raw = ssc.queueStream(rdds, oneAtATime=True).map(float)
    value = raw.map(lambda x: ('value', x))
    last_two_sum = raw.window(2) \
                      .transform(lambda rdd: sc.parallelize([rdd.sum()])) \
                      .map(lambda x: ('sum', x))
    output = value.union(last_two_sum).pprint()

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
