package com.makoto.io
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

trait MovieReader extends Logging {
  def readMovieData(inputPath: String, spark: SparkSession): DataFrame = {
    val movieDF = spark.read.textFile(inputPath).toDF();
    val movieParsedDF = movieDF.select(split($"value", "\t")
      .alias("splits"))
      .selectExpr("splits[0] AS user_id", "splits[1] AS movie_id", "splits[2] AS rating");
    movieParsedDF
  }
  //return map of (movie_id, movie_name)
  def getMovieNameMap(): Map[Int, String] = {
    var map: Map[Int, String] = Map();
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8");
    codec.onMalformedInput(CodingErrorAction.REPLACE);
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE);
    val lines = Source.fromFile("./u.item").getLines();
    for (line <- lines) {
      val fields = line.split('|');
      if (fields.length > 1) {
        //if valid format, add key value pair (movieId,movieName)
        map += (fields(0).toInt -> fields(1).trim());
      }
    }
    map
  }
  /*
  def readMovieTranslation(inputPath: String, spark: SparkSession): DataFrame = {
    val movieTransDF = spark.read.textFile(inputPath).toDF();
    val movieTransParsedDF = movieTransDF.select(split($"value", "|")
                               .alias("splits"))
                               .selectExpr("splits[0] AS movie_id", "splits[1] AS movie_name", "splits[2] AS date");
    movieTransParsedDF
  }
  */
}
