package com.thedataincubator.streamingexample
import org.apache.spark.streaming.StreamingContext

object Netcount {
  def job(ssc: StreamingContext, host: String, port: Int) {
    // Create the Socket Text Stream and use it do some processing
    val wordCounts = ssc.socketTextStream(host, port)
                          .flatMap(_.split(" "))
                          .map(x => (x, 1))
                          .reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
