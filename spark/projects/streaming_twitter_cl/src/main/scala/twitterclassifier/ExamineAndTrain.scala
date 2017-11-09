package twitterclassifier

import org.apache.log4j.{Level, Logger}
import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

/**
 * Examine the collected tweets and trains a model based on them.
 */
object ExamineAndTrain {
  val jsonParser = new JsonParser()
  val gson = new GsonBuilder().setPrettyPrinting().create()

  case class ParsedArg(
    tweetInput: String,
    modelDirectory: String,
    numClusters: Int,
    numIterations: Int
  )

  def main(args: Array[String]) {
    val parsedArgOpt = args match {
      case Array(tweetInput, modelDirectory, numClusters, numIterations) => {
        Try({
          ParsedArg(
            tweetInput,
            modelDirectory,
            numClusters.toInt,
            numIterations.toInt
          )
        }).toOption
      }
      case _ => None
    }

    if (parsedArgOpt == None) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        " <tweetInput> <modelDirectory> <numClusters> <numIterations>")
      System.exit(1)
    }
    val parsedArg = parsedArgOpt.get

    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Pretty print some of the tweets.
    val tweets = sc.textFile(parsedArg.tweetInput)
    println("------------Sample JSON Tweets-------")
    for (tweet <- tweets.take(5)) {
      println(gson.toJson(jsonParser.parse(tweet)))
    }

    val tweetTable = sqlContext.read.json(parsedArg.tweetInput).cache()
    tweetTable.registerTempTable("tweetTable")

    println("------Tweet table Schema---")
    tweetTable.printSchema()

    println("----Sample Tweet Text-----")
    sqlContext.sql("SELECT text FROM tweetTable LIMIT 10").collect().foreach(println)

    println("------Sample Lang, Name, text---")
    sqlContext.sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 1000").collect().foreach(println)

    println("------Total count by languages Lang, count(*)---")
    sqlContext.sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 25").collect.foreach(println)

    println("--- Training the model and persist it")
    val texts = sqlContext.sql("SELECT text from tweetTable").rdd.map(_.toString)
    // Cache the vectors RDD since it will be used for all the KMeans iterations.
    val vectors = texts.map(Featurize.featurize).cache()
    vectors.count()  // Calls an action on the RDD to populate the vectors cache.
    val model = KMeans.train(vectors, parsedArg.numClusters, parsedArg.numIterations)
    sc.makeRDD(model.clusterCenters, parsedArg.numClusters).saveAsObjectFile(parsedArg.modelDirectory)

    val some_tweets = texts.take(100)
    println("----Example tweets from the clusters")
    for (i <- 0 until parsedArg.numClusters) {
      println(s"\nCLUSTER $i:")
      some_tweets.foreach { t =>
        if (model.predict(Featurize.featurize(t)) == i) {
          println(t)
        }
      }
    }
  }
}
