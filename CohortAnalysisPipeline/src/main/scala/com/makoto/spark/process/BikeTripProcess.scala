package com.makoto.spark.process

import com.makoto.spark.conf.Configuration
import com.makoto.spark.io.BikeTripReader
import com.makoto.spark.Util.Util
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//process that compute the current day's bike share trip's average duration
// of each user
object BikeTripProcess extends Logging with BikeTripReader {
  //groupBy fields, groupBy each user to compute each user's average bike
  // duration in the given date
  val fields = List("user_id",
    "subscriber_type",
    "start_station_id",
    "end_station_id",
    "zip_code")
  //average duration column name
  val avgDurationSec = "avg_duration_sec"

  //main
  def main(args: Array[String]): Unit = {
    val conf = new Configuration(args)
    val spark = SparkSession
      .builder()
      .appName("Bike-share")
      .getOrCreate()

    val outputPath = Util.generatePathByDate(conf.outputBikeTripPath(), conf.datePrefix
    (), conf.processDate())

    bikeShareAgg(spark, conf, outputPath)
  }

  //compute the average duration and output override the original data file
  def bikeShareAgg(spark: SparkSession, conf: Configuration, outputPath: String): Unit = {

    val bikeShareDf = getBikeShareDataDF(conf, spark)

    val bikeShareAggDf = bikeShareDf
      .groupBy(fields.map(col):_*)
      .agg(avg(col("duration_sec")).as(avgDurationSec))

    bikeShareAggDf.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }
}
