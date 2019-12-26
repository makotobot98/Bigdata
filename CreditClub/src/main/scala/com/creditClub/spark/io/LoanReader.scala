package com.creditClub.spark.io

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import com.creditClub.spark.types.LoanType

trait LoanReader extends Logging {

  def readLoanData(inputPath: String, spark: SparkSession): Dataset[LoanType] = {

    import spark.implicits._

    //***care performance for csv injestion
    //since we don't have csv data schema predefined, have to scan the whole file to get the schema
    //and second schema for data ingestion
    //--> preferred convering to parquet directly
    val rawData = spark.read.option("header", "true").csv(inputPath)

    //logging info
    logInfo("reading data from %s".format(inputPath))

    val filteredRawDf = rawData
      .filter($"loan_status" =!= "Fully Paid") //filter not fully paid

    //predefined fields to be selected, convert into a column using col, ref: spark.sql.functions.col
    val fields = List("loan_amnt", "term", "int_rate", "installment", "home_ownership",
      "annual_inc", "emp_length", "title", "addr_state", "loan_status", "tot_coll_amt")
      .map(col)

    //select fields, and generate column specifying if has collection, and compute DTI = installment / (annual_inc / 12)
    filteredRawDf.select(fields: _*)
      .withColumn("has_collection", when($"tot_coll_amt" =!= "0", 1).otherwise(0)
        .as("has_collection"))
      .withColumn("DTI", $"installment"/($"annual_inc"/12))
      .drop("loan_status")
      .drop("tot_coll_amt")
      .as[LoanType]
  }
}
