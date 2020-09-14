package com.nir.sercureworks.assignment.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.SparkConf
import com.nir.sercureworks.assignment.jobs._
import org.apache.spark.sql.SparkSession

object Driver {

  val Log = Logger.getLogger("SecureworksAssignment")
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    try{
    Log.info("JOB START: Starting Secureworks_Assignment Driver...")

    val conf = new SparkConf()
      .setAppName("SecureWorksAssignment")
      .setMaster("local")

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()

    //Read the parameter file from the path provided during spark-submit
    if (args.length == 0) {
      Log.error("JOB FAILURE: Parameter file path argument is NULL..")
      throw new IllegalArgumentException("Parameter file path argument is NULL...")
    }
    else{
      Log.info("JOB PROCESS STEP: -----------PARAMETER FILE READ-------------")
      Config.loadConfig(args(0))
    }

    //Downloading file from NASA open-source FTP URI
    Log.info("JOB PROCESS STEP: -----------NASA ACCESS LOG FILE DOWNLOAD-----------")
    val filePath = NasaAccessLogFileDownload.download()

    //Spark process of the access log file
    Log.info("JOB PROCESS STEP: --------SPARK PROCESS TO GET TOP N FREQUENT VISITORS PER DAY----------")
    GetTopNFreqVisitors.run(filePath, spark)

    //Clear cache and stop spark session
    spark.sqlContext.clearCache()
    spark.stop()
    Log.info("JOB END: Exiting Secureworks_Assignment Driver.")
  }
    catch {
      case e: Exception =>
        Log.error("Exception Occurred - " + e.printStackTrace())
        throw new SparkException("")
    }
    }
}
