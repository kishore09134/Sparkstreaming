package org.scala.mcit.project5
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}

trait Main {
  val spark: SparkSession =
    SparkSession.builder().appName("SparkSQL PROJECT").master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))

  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
}
