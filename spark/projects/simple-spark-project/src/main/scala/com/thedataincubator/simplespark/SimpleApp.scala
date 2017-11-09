package com.thedataincubator.simplespark

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    if (args.length != 1) {
        System.err.println("Requires HDFS input as argument")
        System.exit(1)
    }
    val logFile = args(0)
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, minPartitions=2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("#" * 80)
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    println("#" * 80)
  }
}
