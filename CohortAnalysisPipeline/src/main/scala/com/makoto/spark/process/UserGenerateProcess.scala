package com.makoto.spark.process

import org.apache.spark.internal.Logging
import com.makoto.spark.conf.Configuration
import com.makoto.spark.Util.Util
import com.makoto.spark.io.{BikeTripReader, UserReader}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
/*
* - UserGenerateProcess takes the cml argument:
*     1. input/output user folder path,
*     2. input path for current date's bikeshare data
* - Steps:
*     1. obtain user lists, (user_id, starting_date)
*     2. obtain bikeshare data
*       2.1 convert bikeshare df into user df (same shcema from 1)
*     3. union two df by (user_id)
*     4. output(overwrite) to the output user folder path
* */
object UserGenerateProcess extends Logging with BikeTripReader with UserReader{
  def main(args: Array[String]): Unit = {
    logInfo("Running user process")

    val spark = SparkSession.builder().appName("Generating Unique User List")
      .getOrCreate()
    val conf: Configuration = new Configuration(args);
    //output path is the same as input path for input unique user path
    val outputPath: String = conf.userListPath();
    writeUniqueUser(outputPath, conf, spark)
  }

  //given output path, generate the uniqueUser DataFrame and write to the path
  def writeUniqueUser(outputPath: String, conf: Configuration,
                      spark: SparkSession): Unit = {

    val oldUserDF = getUserListDF(conf, spark)
    val bikeShareDF = getBikeShareDataDF(conf, spark)
    //select (user_id, start_timestamp)
    val newUserDF = Util.selectDataFrameColumns("bike.unique.user", conf, bikeShareDF)

    //union old and new userDF by user_id, then deduplicate
    //also preserve the initial timestamp that the user subscribed
    //Please, Use UnionByName, for the ordered columns
    val unionUserDF = oldUserDF.unionByName(newUserDF).toDF().groupBy("user_id")
      .agg(min("start_timestamp").as("start_timestamp"))

    //write results
    unionUserDF.distinct().coalesce(1).write.mode(SaveMode
      .Overwrite)
      .json(outputPath)
  }
}
