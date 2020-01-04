package com.makoto.spark.io

import com.makoto.spark.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.makoto.spark.Util.Util
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


trait BikeTripReader extends Logging {
  //read bike share trip df with path from configuration, if not found return
  // a empty dataframe
  def getBikeShareDataDF(conf: Configuration, spark: SparkSession): DataFrame = {
    val path = Util.generatePathByDate(conf.inputBikeSharePath(), conf
      .datePrefix(),conf.processDate())
    logInfo(s"reading bike trip data from $path")

    val bikeShareDF = try {
      Some(spark.read.json(path)).get
    } catch {
      case e: Exception => spark.emptyDataFrame.withColumn("user_id", lit(null: StringType))
        .withColumn("subscriber_type", lit(null: StringType))
        .withColumn("start_station_id", lit(null: StringType))
        .withColumn("end_station_id", lit(null: StringType))
        .withColumn("zip_code", lit(null: StringType))
        .withColumn("duration_sec", lit(null: DoubleType))
        .withColumn("start_timestamp", lit(null: StringType))
    }
    //select fields specified in conf
    Util.selectDataFrameColumns("bike.share.trip" ,conf , bikeShareDF)
  }
  def getBikeTripDayAgoDF(conf: Configuration, spark: SparkSession): DataFrame = {
    val path = dayAgoReadDataOutPath(conf)

    logInfo("reading from %s".format(path))

    val bikeShareDf: DataFrame = try {
      Some(spark.read.json(path)).get
    } catch {
      case e: Exception => spark.emptyDataFrame
        .withColumn("user_id", lit(null: StringType))
        .withColumn("subscriber_type", lit(null: StringType))
        .withColumn("start_station_id", lit(null: StringType))
        .withColumn("end_station_id", lit(null: StringType))
        .withColumn("zip_code", lit(null: StringType))
        .withColumn("avg_duration_sec", lit(null: DoubleType))
    }
    bikeShareDf
  }
  //return the dayAgo read bike trip path string
  def dayAgoReadDataOutPath(conf: Configuration): String = {
    val dateString = dayAgoDateString(conf)

    val path : String = conf.dayAgo() match {
      case 1 => Util.generatePathByDate(conf.outputBikeTripPath(), conf.datePrefix(), dateString)
      case 3 => Util.generatePathByDate(conf.outputBikeTripPath()+"/1", conf.datePrefix(), dateString)
      case 7 => Util.generatePathByDate(conf.outputBikeTripPath()+"/3", conf.datePrefix(), dateString)
      case _ => throw new Exception("input date is invalid")
    }
    path
  }
  //return the dayAgo write bike trip path string
  def dayAgoWriteDataOutPath(conf: Configuration): String = {
    val dateString = dayAgoDateString(conf)

    val path : String = conf.dayAgo() match {
      case 1 => Util.generatePathByDate(conf.outputBikeTripPath()+"/1", conf.datePrefix(), dateString)
      case 3 => Util.generatePathByDate(conf.outputBikeTripPath()+"/3", conf.datePrefix(), dateString)
      case 7 => Util.generatePathByDate(conf.outputBikeTripPath()+"/7", conf.datePrefix(), dateString)
      case _ => throw new Exception("input date is invalid")
    }
    path
  }
  //return the date, computed with day ago(in conf) subtracted from current date
  def dayAgoDateString(conf: Configuration): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val processDate: DateTime         = DateTime.parse(conf.processDate(), dateFormat)
    dateFormat.print(processDate.minusDays(conf.dayAgo()))
  }
}