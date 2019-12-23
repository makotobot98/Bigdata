package com.makoto.io
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

trait MovieScoreWriter extends Logging{
  def writeSimilarityData(outputDataframe: DataFrame, outputPath: String): Unit
  = {
    outputDataframe.repartition(1).write.json(outputPath)
  }
}
