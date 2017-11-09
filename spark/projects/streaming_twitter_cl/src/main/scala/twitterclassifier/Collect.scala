package twitterclassifier

import java.io.File
import org.apache.log4j.{Level, Logger}
import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try
import twitter4j.auth.OAuthAuthorization


/**
 * Collect at least the specified number of tweets into json text files.
 */
object Collect {
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  case class ParsedArg(
    twitterCredentials: Option[OAuthAuthorization],
    outputDirectory: String,
    numTweets: Int,
    intervalInSeconds: Int,
    numPartitions: Int
  )

  def main(args: Array[String]) {
    // Process program arguments and set properties
    val parsedArgOpt = args match {
      case Array(twitterCredentialsFile, outputDirectory, numTweetsStr, intervalInSecondsStr, numPartitionsStr) => {
        Try({
          ParsedArg(
            TwitterCredentials.parseTwitterCredentials(twitterCredentialsFile),
            outputDirectory,
            numTweetsStr.toInt,
            intervalInSecondsStr.toInt,
            numPartitionsStr.toInt
          )
        }).toOption
      }
      case _ => None
    }

    if (parsedArgOpt == None) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        "<twitterCredentialsFile> <outputDirectory> <numTweets> <intervalInSeconds> <numPartitions>")
      System.exit(1)
    }
    val parsedArg = parsedArgOpt.get

    if (new File(parsedArg.outputDirectory.toString).exists()) {
      System.err.println("ERROR - %s already exists: delete or specify another directory".format(
        parsedArg.outputDirectory))
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(parsedArg.intervalInSeconds))

    val tweetStream = TwitterUtils.createStream(ssc, parsedArg.twitterCredentials)
      .map(gson.toJson(_))

    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(parsedArg.numPartitions)
        outputRDD.saveAsTextFile(parsedArg.outputDirectory + "/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > parsedArg.numTweets) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
