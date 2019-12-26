package com.creditClub.spark

import com.creditClub.spark.aggregator.LoanInfoAggregator
import io.{LoanAggregationWriter, LoanReader, RejectionReader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object LoanAnalyze extends Logging with LoanReader with RejectionReader with LoanInfoAggregator with LoanAggregationWriter {

  def main (args: Array[String]): Unit = {

    if (args.length != 3) {
      println("need three parameters")
    }

    val spark = SparkSession
      .builder()
      .appName("Credit-analysis")
      .getOrCreate()

    val loanInputPath = args(0)
    val rejectionInputPath = args(1)
    val outputPath = args(2)
    /*
    pipeline:
      1. read and standardize load data: compute missing columns for the DS fields(DTI, has_collection)
      2. read and standardize rejection loan data: fill nulls for missing columns
      3. union two datasets and group by the same user/loan profile, to generate the analyzed results for each user/loan profile
      4. write results as JSON
    */
    //from LoanReader.scala
    val loanDs = readLoanData(loanInputPath, spark)

    //from RejectionReader.scala
    val rejectionDs = readRejectionData(rejectionInputPath, spark)

    //from LoanInfoAggregator.scala
    val aggregatedDf = loanInfoAggregator(rejectionDs, loanDs, spark)

    //from LoanAggregationWriter.scala
    writeLoanAggregatedData(aggregatedDf, outputPath)
  }

}
