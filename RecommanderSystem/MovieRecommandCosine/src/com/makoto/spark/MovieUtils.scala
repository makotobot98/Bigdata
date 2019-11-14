package com.makoto.spark


import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec


trait MovieUtils {
    //TODO: get (movieID, movieName) for mapping id to actual movie names
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
}