package com.cdtian

import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

/**
  * @author tianyj3@lenovo.com
  * @date 2019/9/3.
  * @copyright Copyright Lenovo Corporation 2018 All Rights Reserved.
  */
object DeequApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("deequ job").getOrCreate()
    val dataSet = spark.read.textFile("/tmp/tpcds-generate/10/time_dim")
    dataSet.show(100)
    import spark.implicits._
    val newDataSet = dataSet.map(_.split("\\|")).map(attrs => Time_Dim(attrs(0).toLong, attrs(1), attrs(2).toInt, attrs(3).toInt, attrs(4).toInt, attrs(5).toInt, attrs(6), attrs(7), attrs(8), attrs(8)))(Encoders.product[Time_Dim])
    val dataDf = newDataSet.toDF()
    val analysisResult: AnalyzerContext = {
      AnalysisRunner.onData(dataDf)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("t_time_sk"))
        .addAnalyzer(Completeness("t_time_id"))
        .addAnalyzer(ApproxCountDistinct("t_time_sk"))
        .addAnalyzer(ApproxCountDistinct("t_time"))
        .addAnalyzer(ApproxCountDistinct("t_hour"))
        .addAnalyzer(ApproxCountDistinct("t_minute"))
        .addAnalyzer(CountDistinct("t_hour"))
        .addAnalyzer(DataType("t_hour"))
        .addAnalyzer(Distinctness("t_hour"))
        .addAnalyzer(MutualInformation(Seq("t_hour", "t_minute")))
        .addAnalyzer(ApproxCountDistinct("t_second"))
        .addAnalyzer(ApproxCountDistinct("t_second"))
        .run()
    }

    val metrics = successMetricsAsDataFrame(spark, analysisResult);
    metrics.show();

  }

  case class Time_Dim(
                       t_time_sk: Long,
                       t_time_id: String,
                       t_time: Int,
                       t_hour: Int,
                       t_minute: Int,
                       t_second: Int,
                       t_am_pm: String,
                       t_shift: String,
                       t_sub_shift: String,
                       t_meal_time: String
                     )

}

