package com.thedataincubator.streamingexample

import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

object Shakespeare {
  val shakespeare = """
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
    """.split("\n")
        .map(_.trim)
        .filter( _ != "")

  def updateFunc(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    Some(runningCount.getOrElse(0) + newValues.sum)
  }

  def job(ssc: StreamingContext) {
    ssc.checkpoint("/tmp/")   // set checkpoint

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = Queue[RDD[String]](shakespeare
      .map(line => ssc.sparkContext.makeRDD(Seq(line))): _*
    )
    // Create the QueueInputDStream and use it do some processing
    val wordStream = ssc.queueStream(rddQueue, oneAtATime=true)
                          .flatMap(_.split(" "))
                          .map( x => (x, 1))
                          .reduceByKey(_ + _)

    val stateDstream = wordStream.updateStateByKey[Int](updateFunc _)
    stateDstream.print()

    // To write results to a file, use this
    // stateDstream.saveAsTextFiles("/tmp/output/file")

    ssc.start()
    ssc.awaitTermination()
  }
}