package com.creditClub.spark.io

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

trait LoanAggregationWriter extends Logging {

  def writeLoanAggregatedData(outputDataframe: DataFrame, outputPath: String): Unit = {
    outputDataframe.repartition(1).write.json(outputPath)
  }
}
