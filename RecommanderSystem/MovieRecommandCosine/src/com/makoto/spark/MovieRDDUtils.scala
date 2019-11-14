package com.makoto.spark

import scala.math.sqrt

class MovieRDDUtils {
  //MovieRatingPair: key = movieId, value = Movie rating
  type MovieRatingPair = (Int, Double);
  //UserRatingPair: container with key = userID, value = TWO ratings of this user
  type UserRatingPair = (Int, (MovieRatingPair, MovieRatingPair));
  //RatingPair: just a Pair of two ratings
  type RatingPair = (Double, Double);
  //container for a list of RatingPair for computing the cosine similarity
  type RatingPairs = Iterable[RatingPair];
  
  //map ((movie1, rating1), (movie2, rating2)) into ((movie1, movie2), (rating1, rating2))
  // so we can use (movie1, movie2) as keys to group by all (movie1, movie2) ratings by all users
  def makePairs(userRatings:UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2
    
    ((movie1, movie2), (rating1, rating2))
  }
  
  //helper function to deduplicate from self join
  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val movie1 = movieRating1._1
    val movie2 = movieRating2._1
    
    return movie1 < movie2
  }
  
  //cosine similarity: https://en.wikipedia.org/wiki/Cosine_similarity
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
  
}