package com.matteoguarnerio.r3pi

import org.apache.spark.sql._
import org.apache.spark.SparkConf

object SparkCommons {
  lazy val driverPort = 7777
  lazy val driverHost = "localhost"

  lazy val conf: SparkConf = new SparkConf()
    .setMaster("local[*]") // run locally with as many threads as CPUs
    .setAppName("R3PI - Assignment") // name in web UI
    .set("spark.driver.port", driverPort.toString)
    .set("spark.driver.host", driverHost)
    .set("spark.local.dir", "tmpspark")
    .set("spark.logConf", "true")

  lazy val sparkSession: SparkSession = SparkSession.builder
    .config(conf = conf)
    .appName("Spark SQL session")
    .getOrCreate()

}
