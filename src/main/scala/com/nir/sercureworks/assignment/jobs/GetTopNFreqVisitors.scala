package com.nir.sercureworks.assignment.jobs

import org.apache.log4j.{Level, Logger}
import com.nir.sercureworks.assignment.core.Constants
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GetTopNFreqVisitors {

  val Log = Logger.getLogger("SecureworksAssignment")
  Log.setLevel(Level.DEBUG)

  def run(filePath: String, spark: SparkSession) {

    try {
      //Regex pattern to parse the NASA access log file (Common Log Format) - remotehost rfc931 authuser [date] "request" status bytes
      val pattern = "^(\\S+) (\\S+) (\\S+) " +
        "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +
        " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)"

      //Spark read of NASA access log file
      //Parse the NASA access log file using regex pattern
      Log.info("JOB PROCESS: Starting Spark read and parsing of NASA Access log file: " + filePath)
      spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
      val df = spark.read.textFile(s"file://${filePath}").select(
        to_timestamp(trim(regexp_extract(col("value"), pattern, 4)), "dd/MMM/yyyy:HH:mm:ss ZZZZ").cast("date").alias("date"),
        trim(regexp_extract(col("value"), pattern, 1)).alias("visitor_url"),
        trim(regexp_extract(col("value"), pattern, 3)).alias("visitor_username"),
        lit(1).cast("int").alias("row_num")
      ).filter(col("date").isNotNull)
        .repartition(100)
      // .limit(1000)
      df.cache()
      Log.info("JOB PROCESS: Total count of visits in the NASA Access Log file: " + df.count())
      Log.info("JOB PROCESS: Completed Spark read and parsing of NASA Access log file")

      //Window function specifications for aggregations and ranking
      val windowSpecAgg = Window.partitionBy(col("visitor_url"), col("visitor_username"), col("date"))
      val windowSpecTopN = Window.partitionBy(col("date")).orderBy(col("totalVisitPerDay") desc)

      //Top-n most frequent visitors and urls for each day of the trace
      //Aggregation and Ranking
      Log.info("JOB PROCESS: Starting to Compute Top " + Constants.TOP_N_VALUE + " Frequent Visitors & URLs Per Day")
      val topNdf = df.withColumn("totalVisitPerDay", sum(col("row_num")).over(windowSpecAgg))
        .drop(col("row_num")).dropDuplicates()
        .withColumn("rnk", dense_rank().over(windowSpecTopN))
        .filter(col("rnk") <= Constants.TOP_N_VALUE)
      //Print the output along with total number of visits per day and associated rank for each visitor_url & visitor_username
      Log.info("JOB PROCESS: Top " + Constants.TOP_N_VALUE + " Frequent Visitors & URLs Per Day" + topNdf.show(topNdf.count().toInt,false))
    }
    catch {
      case e: Exception =>
        Log.error("Exception Occurred - " + e.printStackTrace())
        throw new SparkException("")
    }
  }
}
