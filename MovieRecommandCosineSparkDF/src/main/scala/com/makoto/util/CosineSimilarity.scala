package com.makoto.util
import org.apache.spark.sql.{DataFrame, SparkSession};
import com.makoto.io.MovieReader;
import org.apache.spark.sql.functions.sum;

trait CosineSimilarity {
  //output the dataframe with similarity score between movie pairs
  def computeCosineSimilarity(movieDF: DataFrame, spark: SparkSession): DataFrame = {
    val movieDF2 = movieDF.selectExpr("*").selectExpr("user_id", "movie_id AS movie_id2", "rating AS rating2");
    val moviePairDF = movieDF.join(movieDF2, "user_id").withColumnRenamed("movie_id", "movie_id1").withColumnRenamed("rating", "rating1");
    //filter out so movie_id1 < movie_id2, since pair(1, 2) and (2, 1) are the same, we only need to preserve one of it
    val filteredMoviePairDF = moviePairDF.where("movie_id1 < movie_id2");
    val cosineDistMovieDF = filteredMoviePairDF.selectExpr("*","rating1 * rating1 AS xx", "rating2 * rating2 AS yy", "rating1 * rating2 AS xy");
    //group pairs with same (movie_id1, movie_id2), and aggregate xx, yy, xy
    // for cosine similarity computation
    val groupedCosineDistDF = cosineDistMovieDF.groupBy("movie_id1",
      "movie_id2").agg(sum("xx").alias("sum_xx"), sum("yy").alias("sum_yy"),
      sum("xy").alias("sum_xy"));
    //compute similarity
    val similarityDF = groupedCosineDistDF.selectExpr("movie_id1",
      "movie_id2", "(sum_xy / (sqrt(sum_xx) + sqrt(sum_yy))) AS " +
        "similarity_score");
    similarityDF
  }
}
