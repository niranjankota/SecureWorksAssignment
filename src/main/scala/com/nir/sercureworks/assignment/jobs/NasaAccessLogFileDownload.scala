package com.nir.sercureworks.assignment.jobs

import org.apache.log4j.{Level, Logger}
import com.nir.sercureworks.assignment.core.Constants
import java.net.URL
import java.io.File
import sys.process._
import org.apache.spark.SparkException

object NasaAccessLogFileDownload  {

  val Log = Logger.getLogger("SecureworksAssignment")
  Log.setLevel(Level.DEBUG)

  def download(): String = {
    try{
    //Downloading file from NASA open-source FTP URI
    val ftpFilePath = Constants.FTP_FILE_DOWNLOAD_URL
    Log.info("JOB PROCESS: Starting NASA Access Log file downlaod from path: " + Constants.FTP_FILE_DOWNLOAD_URL)
    val url = new URL(ftpFilePath)
    val tokens = ftpFilePath.split("/")
    val fileName = tokens(tokens.length - 1)
    Log.info("JOB PROCESS: NASA Access Log file Name: " + fileName)
    val filePath = Constants.TARGET_FILE_DOWNLOAD_PATH + "/" + fileName
    Log.info("JOB PROCESS: Downloading NASA Access Log file to: " + filePath)
    url #> new File(s"${filePath}") !!

    Log.info("JOB PROCESS: Completed NASA Access Log file downlaod" )

    return filePath
  }
  catch {
    case e: Exception =>
      Log.error("Exception Occurred - " + e.printStackTrace())
      throw new SparkException("")
  }
  }
}
