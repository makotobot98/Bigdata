package com.makoto.spark.io

import com.makoto.spark.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
trait UserReader {
  //from input path in configuration, return the dataframe with data read
  def getUserListDF(conf: Configuration, spark: SparkSession): DataFrame = {
    val path = conf.userListPath();
    val df = try {
      Some(spark.read.json(path)).get
    } catch {
      //if first time reading the user list, simply create empty df (user_id,
      // start_timestamp)
      case e: Exception => spark.emptyDataFrame.select(lit(null: StringType).as
      ("user_id"), lit(null: StringType).as("start_timestamp"))
    }
    df
  }
}
