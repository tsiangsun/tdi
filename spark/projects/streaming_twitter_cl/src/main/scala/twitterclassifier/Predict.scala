package twitterclassifier

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.Try
import twitter4j.auth.OAuthAuthorization

/**
 * Pulls live tweets and filters them for tweets in the chosen cluster.
 */
object Predict {
  case class ParsedArg(
    twitterCredentials: Option[OAuthAuthorization],
    modelDirectory: String,
    clusterNumber: Int,
    intervalInSeconds: Int
  )

  def main(args: Array[String]) {
    val parsedArgOpt = args match {
      case Array(twitterCredentialsFile, modelDirectory, clusterNumber, intervalInSeconds) =>
        Try({
          ParsedArg(
            TwitterCredentials.parseTwitterCredentials(twitterCredentialsFile),
            modelDirectory,
            clusterNumber.toInt,
            intervalInSeconds.toInt
          )
        }).toOption
      case _ => None
    }

    if (parsedArgOpt == None) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <twitterCredentialsFile> <modelDirectory> <clusterNumber> <intervalInSeconds>")
      System.exit(1)
    }
    val parsedArg = parsedArgOpt.get

    Logger.getRootLogger.setLevel(Level.WARN)

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))

    println("Initializing Twitter stream...")
    val tweets = TwitterUtils.createStream(ssc, parsedArg.twitterCredentials)
    val statuses = tweets.map(_.getText)

    println("Initalizaing the the KMeans model...")
    val model = new KMeansModel(ssc.sparkContext.objectFile[Vector](parsedArg.modelDirectory).collect())

    val filteredTweets = statuses
      .filter(t => model.predict(Featurize.featurize(t)) == parsedArg.clusterNumber)
    filteredTweets.print()

    // Start the streaming computation
    println("Initialization complete.")
    ssc.start()
    ssc.awaitTermination()
  }
}
