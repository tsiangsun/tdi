from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

shakespeare = """
    To be, or not to be- that is the question:
    Whether 'tis nobler in the mind to suffer
    The slings and arrows of outrageous fortune
    Or to take arms against a sea of troubles,
    And by opposing end them. To die- to sleep-
    No more; and by a sleep to say we end
    The heartache, and the thousand natural shocks
    That flesh is heir to. 'Tis a consummation
    Devoutly to be wish'd. To die- to sleep.
    To sleep- perchance to dream: ay, there's the rub!
    For in that sleep of death what dreams may come
    When we have shuffled off this mortal coil,
    Must give us pause. There's the respect
    That makes calamity of so long life.
    For who would bear the whips and scorns of time,
    Th' oppressor's wrong, the proud man's contumely,
    The pangs of despis'd love, the law's delay,
    The insolence of office, and the spurns
    That patient merit of th' unworthy takes,
    When he himself might his quietus make
    With a bare bodkin? Who would these fardels bear,
    To grunt and sweat under a weary life,
    But that the dread of something after death-
    The undiscover'd country, from whose bourn
    No traveller returns- puzzles the will,
    And makes us rather bear those ills we have
    Than fly to others that we know not of?
    Thus conscience does make cowards of us all,
    And thus the native hue of resolution
    Is sicklied o'er with the pale cast of thought,
    And enterprises of great pith and moment
    With this regard their currents turn awry
    And lose the name of action.- Soft you now!
    The fair Ophelia!- Nymph, in thy orisons
    Be all my sins rememb'red.
    """

shakespeare = filter(None, [line.strip() for line in shakespeare.split("\n")])

def update(new_vals, running_count):
    if running_count is None:
        running_count = 0
    return running_count + sum(new_vals)

def main():
    conf = SparkConf().setAppName("shakespearecount")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)

    ssc.checkpoint("/tmp/")

    rdds = [sc.parallelize([line]) for line in shakespeare]

    word_stream = ssc.queueStream(rdds, oneAtATime=True) \
                     .flatMap(lambda line: line.split(" ")) \
                     .map(lambda x: (x, 1)) \
                     .reduceByKey(lambda x, y: x + y)

    state_Dstream = word_stream.updateStateByKey(update)
    state_Dstream.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False)).pprint()

    # To write results to a file, use this
    # state_Dstream.saveAsTextFiles("/tmp/output/")

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
