from multiprocessing import Process, Queue
import time
import signal
import socket
import sys

import requests
from requests_oauthlib import OAuth1
import ujson as json

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.linalg import Vectors

PORT = 9876
DIM = 1000
N = 5

def get_twitter_auth(secrets):
    secrets = json.load(open(secrets, "r"))
    return OAuth1(secrets["api_key"], secrets["api_secret"], secrets["access_token"],
                  secrets["access_token_secret"])

def twitter(queue, secrets):
    auth = get_twitter_auth(secrets)

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", PORT))
    sock.listen(1)
    conn, addr = sock.accept()

    stream = requests.get('https://stream.twitter.com/1.1/statuses/sample.json',
                          auth=auth, stream=True)
    for line in stream.iter_lines():
        if not line:  # Keep-alive
            continue

        if queue.empty():
            conn.sendall(line + '\n')
        else:
            break
    conn.close()

def featurize(s):
    return [s[i:i+2] for i in xrange(len(s) - 1)]

def main():
    conf = SparkConf().setAppName("twitterclassifier")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)

    tweets = ssc.socketTextStream("localhost", PORT) \
                .map(lambda x: json.loads(x)) \
                .filter(lambda x: 'text' in x) \
                .map(lambda x: x['text'].encode('utf-8'))
    hasher = HashingTF(DIM)
    features = tweets.map(lambda x: (x, hasher.transform(featurize(x)))).cache()

    # We create a model with random clusters and specify the number of clusters to find
    # decay = 1: total memory; decay = 0: no memory
    model = StreamingKMeans(k=N, decayFactor=0.1).setRandomCenters(DIM, 1.0, 0)
    model.trainOn(features.map(lambda x: x[1]))
    results = model.predictOnValues(features).cache()

    # Need a closure over i here.
    def print_group(i):
        results.filter(lambda x: x[1] == i).map(lambda x: '%i: %s' % (x[1], x[0])).pprint(3)
    for i in xrange(N):
        print_group(i)

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print "Must pass path to twitter secrets file as argument!"
        sys.exit(1)

    queue = Queue()
    process = Process(target=twitter, args=(queue, sys.argv[1]))
    process.start()

    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        pass

    queue.put(['end'])
