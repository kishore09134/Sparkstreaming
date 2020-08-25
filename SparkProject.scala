package org.scala.mcit.project5
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.streaming.dstream.{DStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkProject extends App with Main {

  val filename = "/user/winter2020/kishore/project5/trips/trips.csv"
  val filename1 = "/user/winter2020/kishore/project5/calendar_dates/calendar_dates.csv"
  val filename2 = "/user/winter2020/kishore/project5/frequencies/frequencies.csv"

  val tripDf: DataFrame = spark.read.option("header", "true").option("inferschema", "true").csv(filename)
  val calendarDf: DataFrame = spark.read.option("header", "true").option("inferschema", "true").csv(filename1)
  val frequenciesDf: DataFrame = spark.read.option("header", "true").option("inferschema", "true").csv(filename2)

  tripDf.createOrReplaceTempView("trips")
  calendarDf.createOrReplaceTempView("calendars")
  frequenciesDf.createOrReplaceTempView("frequencies")

  val enrichedTripDf: DataFrame = spark.sql(
    """SELECT t.route_id, t.service_id, t.trip_id
      |FROM trips t LEFT JOIN calendars d ON t.service_id = d.service_id
      |LEFT JOIN frequencies f ON t.trip_id = f.trip_id""".stripMargin)

  val kafkaConfig: Map[String, String] = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "kis",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )

  val topic = "stop_times"
  val inStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig))

  import spark.implicits._

  val output: DStream[String] = inStream.map(_.value())
  output.foreachRDD(rdd => {
    val stopTimeDf = rdd.map(_.split(",")).map(t => StopTimeSchema(t(0), t(1), t(2), t(3), t(4))).toDF
    val enrichedStop = enrichedTripDf.join(stopTimeDf, "trip_id")
    enrichedStop.write.mode(SaveMode.Append).csv("/user/hive/" +
      "warehouse/winter2020_kishore.db/EnrichedStopTime")
    enrichedStop.show()
  }
  )
  ssc.start()
  ssc.awaitTermination()
  spark.stop()
}
