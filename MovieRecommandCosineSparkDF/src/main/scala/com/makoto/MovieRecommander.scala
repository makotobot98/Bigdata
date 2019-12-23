package com.makoto
import io.{MovieReader, MovieScoreWriter};
import util.CosineSimilarity;
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class MovieRecommander extends Logging with MovieScoreWriter with
  MovieReader with CosineSimilarity {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Need specify input and output path as parameters");
    }
    val spark = SparkSession.builder().appName("MovieRecommanderCosineDF")
      .getOrCreate();

    val inputPath = args(0);
    val outputPath = args(1);

    val movieDF = readMovieData(inputPath, spark);

    val similarityDF = computeCosineSimilarity(movieDF, spark);

    writeSimilarityData(similarityDF, outputPath);
  }
}
