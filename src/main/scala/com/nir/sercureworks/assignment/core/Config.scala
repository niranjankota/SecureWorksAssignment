package com.nir.sercureworks.assignment.core

import java.util.Properties
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Config {

  var properties: Properties = new Properties()
  def loadConfig(configPath: String) = {

    val Log = Logger.getLogger("SecureworksAssignment")
    Log.setLevel(Level.DEBUG)

    Log.info("JOB CONFIG: Starting Parameter file read from path: " + configPath)
    val path = new Path(configPath)
    val fs = FileSystem.get(new URI(configPath), new Configuration())
    val inputStream = fs.open(path)
    properties.load(inputStream)
    Log.info("JOB CONFIG: Completed Parameter file read")
  }

  def get(key: String): String = {
    if (properties == null)
      StringUtils.EMPTY
    else
      properties.getProperty(key)
  }

  def get(key: String, defValue: String): String = {
    if (properties == null)
      StringUtils.EMPTY

    val result = properties.getProperty(key)
    if (result == null)
      defValue
    else
      result
  }

}