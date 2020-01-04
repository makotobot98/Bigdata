package com.makoto.spark.io

import com.makoto.spark.conf.Configuration
import com.makoto.spark.Util.Util
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

//reader for bike station file
trait BikeStationReader extends Logging{

  def readBikeStation(conf: Configuration, spark: SparkSession): DataFrame = {
    val path = "%s/bike-station-info".format(conf.inputMetaDataPath())

    logInfo("reading from %s".format(path))

    val bikeStationDF = spark.read.json(path)
    Util.selectDataFrameColumns("bike.station.info", conf, bikeStationDF)
  }
}