package com.makoto.spark.process

import com.makoto.spark.conf.Configuration
import com.makoto.spark.io._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/*
* Compute the 1,3,7 day's retention prior to current bike trip
* Steps:
*   1. get global userListDF, current date's bikeTripDF, dayAgoBikeTrip specified by conf
*   2. left join bikeTripDF and userListDF
*     2.1 compute how many days has each user of today has subscribed since their first subscription
*     2.2 filter user_id's that is in dayAgo we are interested in (passed by conf, 1, 3, or 7)
*   3. left join dayAgoBikeTripDF and (2.2)
*     3.1 update day_ago_retention column, 1 be user is in was "retained", 0 be not "retained"
*     3.2 overwrite to the original data path with DF appended with retention results
* */
//RetentionComputeProcess.scala
object RetentionComputeProcess extends Logging with BikeTripReader with UserReader {
  //main
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration(args)
    val spark: SparkSession = SparkSession.builder().appName("Retention " +
      "Computing Process").getOrCreate()
    //join scale will be controlled very small, table will be filtered in
    // many piplines util join
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    val dayAgo: Int = conf.dayAgo()
    logInfo(s"computing retention of $dayAgo day ago")

    //compute retentions of dayAgo specified in conf
    computeRetention(conf, spark)
  }
  def computeRetention(conf: Configuration, spark: SparkSession): Unit = {
    //(user_id, subscriber_type, start_station ... , duration_sec, start_timestamp)
    val userListDF: DataFrame = getUserListDF(conf, spark)
    //(user_id, first_timestamp)
    val bikeTripDF: DataFrame = getBikeShareDataDF(conf, spark).withColumnRenamed("start_timestamp", "first_timestamp")
    //(user_id, subscriber_type, start_station ... , duration_sec, start_timestamp, retention_1, retention_3, retention_7, avg_duration)
    val dayAgoBikeTrip: DataFrame = getBikeTripDayAgoDF(conf, spark)

    //left join column exp
    val userTripJoinExp = bikeTripDF.col("user_id") === userListDF.col("user_id")
    val joinUserTrip: DataFrame = bikeTripDF.join(userListDF, userTripJoinExp, "left").drop(userListDF.col("user_id"))

    import spark.implicits._
    //compute day ago since first subscription date
    val dayAgoJoinDF = joinUserTrip.withColumn("dayAgo",
      floor((unix_timestamp($"start_timestamp", "yyyy-MM-dd'T'HH:mm:ss")
        - unix_timestamp($"first_timestamp", "yyyy-MM-dd'T'HH:mm:ss")) / 86400))

    //filter out rows with day ago == day ago we are computing
    val filterUserTripDF: DataFrame = conf.dayAgo() match {
      case 1 => dayAgoJoinDF.filter(col("user_age_days") === 1)
      case 3 => dayAgoJoinDF.filter(col("user_age_days") === 3)
      case 7 => dayAgoJoinDF.filter(col("user_age_days") === 7)
      case _ => throw new Exception("input date is invalid")
    }

    //since we only need (user_id, day_ago) to operate the left join
    val currentUserTripDF: DataFrame = filterUserTripDF.select($"user_id", $"dayAgo")

    //left join with dayAgo
    val oldNewTripJoinExp = dayAgoBikeTrip.col("user_id") === currentUserTripDF.col("user_id")
    val joinDF = dayAgoBikeTrip.join(currentUserTripDF, oldNewTripJoinExp, "left").drop(currentUserTripDF.col("user_id"))

    //aggregate and append retention results
    val groupByFields: List[String] = BikeTripProcess.fields :+ BikeTripProcess.avgDurationSec
    val aggDF: DataFrame = joinDF.groupBy(groupByFields.map(col) : _*).agg(
      max(when(col("dayAgo") === 1, 1).otherwise(0)).alias("retention_1"),
      max(when(col("dayAgo") === 3, 1).otherwise(0)).alias("retention_3"),
      max(when(col("dayAgo") === 7, 1).otherwise(0)).alias("retention_7")
    )
    //overrite to the original data, by appending the computed retention columns
    val outputPath = dayAgoWriteDataOutPath(conf)
    aggDF.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }
}
