package com.makoto.spark.Util

import com.makoto.spark.conf.Configuration
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object Util extends Logging{
  //generate the path by appending inputpath "{date_prefix}={date_processing}"
  def generatePathByDate(inputPath: String, datePrefix: String,
                         processDate: String): String = {
    s"$inputPath/$datePrefix=$processDate/"
  }

  //returns the dataframe after selecting given fields provided by sourceKey
  //sourceKey: the fields for dataframe to be selected from the config file
  def selectDataFrameColumns(sourceKey: String, conf: Configuration,
                             inputDF: DataFrame) : DataFrame =
  {
    //get fields from config file with given source key
    val fields = getListFromConf(conf.selectColumnsConfigFile(),
      sourceKey).map(col)
    //select given fields in the input dataframe
    val outputDf = inputDF.select(fields: _*)
    outputDf
  }
  //load list from provided config file, with key = provide config key
  def getListFromConf(configFileName: String, configKey: String): List[String] = {
    val ls: List[String] =
    try {
      //implicit converting java list -> scala list
      ConfigFactory.load(configFileName).getStringList(configKey).toList
    } catch {
      case e: Exception =>
        logError(s"*** Error parsing for $configKey as List[String] from " +
          s"$configFileName ***\n${e.getMessage}")
        List[String]()
    }
    ls
  }
}
