package com.matteoguarnerio

import org.apache.spark.sql._
import org.apache.spark.SparkConf

object SparkCommons {
  lazy val driverPort = 7777
  lazy val driverHost = "localhost"

//  lazy val conf: SparkConf = new SparkConf()
//    .setMaster("local[*]") // run locally with as many threads as CPUs
//    .setAppName("R3PI - Assignment") // name in web UI
//    .set("spark.driver.port", driverPort.toString)
//    .set("spark.driver.host", driverHost)
//    .set("spark.local.dir", "tmpspark")
//    .set("spark.logConf", "true")
//
//  lazy val sparkSession: SparkSession = SparkSession.builder
//    .config(conf = conf)
//    .appName("Spark SQL session")
//    .getOrCreate()

  def getSparkSession(fonte: String="test", queue: String="default", qt_exec_master: String = "*"): SparkSession = {
    SparkSession
      .builder()
      .appName("decode-"+ fonte)
      .config("spark.master", "yarn")
      .config("spark.yarn.queue", queue)
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.shuffle.compress", "true")
      .config("spark.shuffle.service.port","7337")
      .config("spark.executor.cores", "2")
      .config("spark.executor.memory", "3G")
      //          .config("spark.yarn.executor.memoryOverhead", "40g")
      //          .config("spark.driver.memory", "300G")
      //          .config("spark.yarn.driver.memoryOverhead", "150g")
      //          .config("spark.sql.shuffle.partitions", "2024")
      //          .config("spark.default.parallelism", "2024")
      //          .config("spark.scheduler.mode", "FIFO")
      //          .config("spark.ui.port", "4066")
      //          .config("spark.default.parallelism", "240")
      //          .config("spark.dynamicAllocation.enabled", "true")
      //          .config("spark.dynamicAllocation.initialExecutors", "8")
      //          .config("spark.dynamicAllocation.minExecutors", "2")
      //          .config("spark.dynamicAllocation.maxExecutors", "120")
      //          .config("spark.hadoop.yarn.resourcemanager.webapp.address", "logistics.redecorp.br:8088")
      .config("tez.queue.name", queue)
      //          .config("mapreduce.job.queuename", queue)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "1500")
      //          .config("spark.sql.broadcastTimeout", "36000")
      .enableHiveSupport()
      .getOrCreate()
  }

    lazy val sparkSession: SparkSession = getSparkSession()

}
