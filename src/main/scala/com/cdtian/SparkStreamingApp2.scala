//package com.cdtian
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * @author tianyj3@lenovo.com
//  * @date 2019/9/5.
//  * @copyright Copyright Lenovo Corporation 2018 All Rights Reserved.
//  */
//object SparkStreamingApp2 {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    val ss= new SparkSession(conf)
//    val ssc = new StreamingContext(conf, Seconds(1))
//    val lines = ssc.socketTextStream("localhost", 1234)
//    val words = lines.flatMap(_.split(" "))
//    val pairs = words.map(word=>(word,1))
//    val wordCounts = pairs.reduceByKey(_+_)
//    wordCounts.print()
//  }
//}
