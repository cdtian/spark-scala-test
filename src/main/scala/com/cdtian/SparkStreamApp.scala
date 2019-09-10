//package com.cdtian
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.StructType
//
///**
//  * @author tianyj3@lenovo.com
//  * @date 2019/9/4.
//  * @copyright Copyright Lenovo Corporation 2018 All Rights Reserved.
//  */
//object SparkStreamApp {
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder
//      .appName("StructuredNetworkWordCount")
//      .getOrCreate()
//
////    import spark.implicits._
////    val lines = spark.readStream.format("socket").option("host", "localhost").option("port",1234).load()
////
////    // Split the lines into words
////    val words = lines.as[String].flatMap(_.split(" "))
//
//    // Generate running word count
////    val wordCounts = words.groupBy("value").count()
////   wordCounts.writeStream.format("socket").option("host","127.0.0.1").option("port","1233").outputMode("complete").start()
//
//    val socketDF = spark
//      .readStream
//      .format("socket")
//      .option("host", "localhost")
//      .option("port", 1234)
//      .load()
//
//    socketDF.isStreaming
//    val userSchema = new StructType().add("name","string").add("age","integer")
//    val csvDF = spark.readStream.option("seq",":").schema(userSchema).csv("")
//
//  }
//
//}
