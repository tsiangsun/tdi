package com.thedataincubator.streamingexample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingExample {
  object Programs extends Enumeration {
    type Programs = Value
    val Netcount, Shakespeare, Window = Value
  }

  implicit val programReads: scopt.Read[Programs.Value] =
    scopt.Read.reads(Programs.withName _)

  case class Config(
    program: Programs.Programs=Programs.Netcount,
    host: String="localhost",
    port: Int=0
  )

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("scopt", "3.x")
      opt[Programs.Programs]('p', "program").required().action{ (x, c) => c.copy(program=x) }
        .text("values must be one of: " + Programs.values.map(_.toString).reduce(_ + ", " + _))
      opt[String]("host").action{ (x, c) => c.copy(host=x) }
      opt[Int]("port").action{ (x, c) => c.copy(port=x) }
    }

    parser.parse(args, Config()).map(config => {
      // Set warning level to not get as many qarnings
      Logger.getRootLogger.setLevel(Level.WARN)

      val sparkConf = new SparkConf().setAppName("Example")
      val ssc = new StreamingContext(sparkConf, Seconds(1))

      config.program match {
        case Programs.Netcount => Netcount.job(ssc, config.host, config.port)
        case Programs.Shakespeare => Shakespeare.job(ssc)
        case Programs.Window => Window.job(ssc)
      }
    })
  }
}
