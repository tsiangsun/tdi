package com.thedataincubator.streamingexample

import org.apache.spark.streaming.StreamingContext
import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds

object Window {
  def job(ssc: StreamingContext) {
    val rddQueue = Queue[RDD[Int]]((1 to 100)
      .map( i => ssc.sparkContext.makeRDD(Seq(i))): _*
    )

    // Create the Socket Text Stream and use it do some processing
    val raw = ssc.queueStream(rddQueue, oneAtATime=true).map(_.toDouble)

    val value = raw.map(x => ("value" -> x))

    val lastTwoSum = raw.window(Seconds(2))
      .transform( rdd => ssc.sparkContext.makeRDD(Seq(rdd.sum)) )
      .map(x => ("lastTwoSum" -> x))

    val output = ssc.union(Seq(value, lastTwoSum)).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
