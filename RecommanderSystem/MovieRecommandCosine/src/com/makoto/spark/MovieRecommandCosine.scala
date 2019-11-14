package com.makoto.spark

import com.makoto.spark.MovieRDDUtils
import com.makoto.spark.MovieUtils
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


/*
 * steps: 
 * 1. read (userID, (movieID, movieRating)) 
 * 2. self join and deduplicate to find the co occurance movie records rated by same user
 * 3. Group all same movie pairs rated by different users (so group all (movie1, movie2) rated by user1, 2, 3 ...)
 * 		we are garanteed the dimension of vectorization of movie1,2 are same, so we can compute cosine similarity
 * 4. compute cosine similarity between each movie pair
 * 5. cache (or push into the database or file system) similarity between each pair of movie on memory
 * 6. take input from command line about which movie to query to get the similar movie recommandation
 * 7. print the result in console
 * */
object MovieRecommandCosine extends MovieRDDUtils with MovieUtils{
  /*
  def getMovieNameMap(): Map[Int, String] = {
    var map: Map[Int, String] = Map();
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8");
    codec.onMalformedInput(CodingErrorAction.REPLACE);
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE);
    //for 1M, from "movies.dat", make sure movies.dat is in the working dir so it's exposed to master
    val lines = Source.fromFile("../ml-100k/u.item").getLines();  
    for (line <- lines) {
      val fields = line.split('|'); //for 1M, split by "::"
      if (fields.length > 1) {
        //if valid format, add key value pair (movieId,movieName)
        map += (fields(0).toInt -> fields(1).trim());
      }
    }
    return map;
  }
  */
  def main(args: Array[String]) {
    
    
    /* use this if run on EMR cluster, use spark configuration from EMR 
    val conf = new SparkConf()
    conf.setAppName("MovieSimilarities1M")
    val sc = new SparkContext(conf)
    */
    //for purpose of testing, running on local
    val sc = new SparkContext("local[*]", "MovieRecommandCosine");
    
    
    val nameMap = getMovieNameMap();
    
    //if running on cluster: val data = sc.textFile("s3n://path/ml-1m/ratings.dat")
    val data = sc.textFile("../ml-100k/u.data");
    
    // Map ratings to key / value pairs: user ID => movie ID, rating
    val ratings = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)));
    
    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings = ratings.join(ratings);   
    
    //  RDD consists of userID => ((movieID, rating), (movieID, rating))

    // filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates);

    // Now map key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs);
    //if run on cluster, specify partition beforehand partitionBy benefits a lot from partitioning
    //val moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(new HashPartitioner(100))

    // now have (movie1, movie2) => (rating1, rating2)
    // group each movie pair with all ratings by different users
    val moviePairRatings = moviePairs.groupByKey();

    // (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // compute cosine similarities, and cache the results in memory
    // *note, with large amount of data, preferred to injest the data into structured database
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache();
    
    //Save the results if needed
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("filename")
    
    
    
    if (args.length > 3) {
      //arg(0) = movie to be lookup up, arg(1) = similarityScoreThreshhold, arg(2) = coOccuranceThreshhold, agr(3) = number of similar movie to recommand
      val similarityScoreThresholdPercent = args(1).toInt;  //.97
      val similarityScoreThreshold = similarityScoreThresholdPercent * 1.0f / 100;
      
      val coOccuranceThreshold = args(2).toInt; //1000 for 1M data, 50 for 100k
      val movieID:Int = args(0).toInt;
      val numToRecommand:Int = args(3).toInt;
      
      println(s"similarity threshold set to $similarityScoreThreshold coOcc set to $coOccuranceThreshold");
      //filter out movies that are specified by similarity and coOccurance thresholds
      val filteredResults = moviePairSimilarities.filter( x =>
        {
          val moviePair = x._1;
          val similarityScore = x._2;
          (moviePair._1 == movieID || moviePair._2 == movieID) && similarityScore._1 > similarityScoreThreshold && similarityScore._2 > coOccuranceThreshold;
        }
      )
        
      // Sort by movies by similarity score in descending order, and fetch numToRecommand number of records
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(numToRecommand)
      
      println(s"\nTop $numToRecommand similar movies for " + nameMap(movieID));
      
      for (result <- results) {
        val similarity = result._1
        val moviePair = result._2
        
        var similarMovieID = moviePair._1
        if (similarMovieID == movieID) {
          similarMovieID = moviePair._2
        }
        println(nameMap(similarMovieID) + "\tscore: " + similarity._1 + "\tstrength: " + similarity._2)
      }
    }
  }
}