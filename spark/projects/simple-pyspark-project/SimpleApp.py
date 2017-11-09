from pyspark import SparkContext, SparkConf
import sys
from datetime import datetime
from myutils import contains

def main(*args):
    if len(args) != 1:
        print "Requires a single HDFS path as argument"
        sys.exit(1)

    fn = args[0]
    conf = SparkConf()
    conf.setAppName("SimpleApp")
    sc = SparkContext(conf=conf)

    data = sc.textFile(fn, 2).cache()
    numAs = data.filter(contains("a")).count()
    numBs = data.filter(contains("b")).count()

    print "#" * 80
    print "Lines with a: {A}, Lines with b: {B}".format(A=numAs, B=numBs)
    print "#" * 80


if __name__ == '__main__':
    main(*sys.argv[1:])
