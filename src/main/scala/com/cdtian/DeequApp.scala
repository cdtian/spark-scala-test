package com.cdtian

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * @author tianyj3@lenovo.com
  * @date 2019/9/3.
  * @copyright Copyright Lenovo Corporation 2018 All Rights Reserved.
  */
object DeequApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("run 1 size all Completeness all ApproxCountDistinct all ApproxQuantile 2 Compliance").getOrCreate()
    val dataSet = spark.read.textFile("/tmp/tpcds-generate/10/time_dim")
    import spark.implicits._
    val newDataSet = dataSet.map(_.split("\\|")).map(attrs => Time_Dim(attrs(0).toLong, attrs(1), attrs(2).toInt, attrs(3).toInt, attrs(4).toInt, attrs(5).toInt, attrs(6), attrs(7), attrs(8), attrs(8)))(Encoders.product[Time_Dim])
    val dataDf = newDataSet.toDF().cache()
    val analysisResult: AnalyzerContext = {
      AnalysisRunner.onData(dataDf)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("t_time_sk")) //非空数据百分比
        .addAnalyzer(Completeness("t_time_id")) //非空数据百分比
        .addAnalyzer(Completeness("t_time")) //非空数据百分比
        .addAnalyzer(Completeness("t_hour")) //非空数据百分比
        .addAnalyzer(Completeness("t_minute")) //非空数据百分比
        .addAnalyzer(Completeness("t_second")) //非空数据百分比
        .addAnalyzer(Completeness("t_am_pm")) //非空数据百分比
        .addAnalyzer(Completeness("t_shift")) //非空数据百分比
        .addAnalyzer(Completeness("t_sub_shift")) //非空数据百分比
        .addAnalyzer(Completeness("t_meal_time")) //非空数据百分比
        .addAnalyzer(ApproxCountDistinct("t_time_sk")) // 统计不同数据个数(接近)
        .addAnalyzer(ApproxCountDistinct("t_time_id")) // 统计不同数据个数(接近)
        .addAnalyzer(ApproxCountDistinct("t_time")) // 统计不同数据个数(接近)
        .addAnalyzer(ApproxCountDistinct("t_hour")) // 统计不同数据个数(接近)
        .addAnalyzer(ApproxCountDistinct("t_minute")) // 统计不同数据个数(接近)
        .addAnalyzer(ApproxCountDistinct("t_second")) // 统计不同数据个数(接近)
        .addAnalyzer(ApproxCountDistinct("t_am_pm")) // 统计不同数据个数(接近)
        .addAnalyzer(ApproxCountDistinct("t_shift")) // 统计不同数据个数(接近)
        .addAnalyzer(ApproxCountDistinct("t_sub_shift")) // 统计不同数据个数(接近)
        .addAnalyzer(ApproxCountDistinct("t_meal_time")) // 统计不同数据个数(接近)
//        .addAnalyzer(CountDistinct("t_time_sk")) // 统计不同数据个数
//        .addAnalyzer(CountDistinct("t_time_id")) // 统计不同数据个数
//        .addAnalyzer(CountDistinct("t_time")) // 统计不同数据个数
        .addAnalyzer(ApproxQuantile("t_time_id", quantile = 0.1)) //基于quantile 区间的分布
        .addAnalyzer(ApproxQuantile("t_time_sk", quantile = 0.2)) //基于quantile 区间的分布
        .addAnalyzer(ApproxQuantile("t_time", quantile = 0.3)) //基于quantile 区间的分布
        .addAnalyzer(ApproxQuantile("t_hour", quantile = 0.4)) //基于quantile 区间的分布
        .addAnalyzer(ApproxQuantile("t_minute", quantile = 0.5)) //基于quantile 区间的分布
        .addAnalyzer(ApproxQuantile("t_second", quantile = 0.6)) //基于quantile 区间的分布
        .addAnalyzer(ApproxQuantile("t_am_pm", quantile = 0.7)) //基于quantile 区间的分布
        .addAnalyzer(ApproxQuantile("t_shift", quantile = 0.8)) //基于quantile 区间的分布
        .addAnalyzer(ApproxQuantile("t_meal_time", quantile = 0.9)) //基于quantile 区间的分布
        .addAnalyzer(Compliance("top 1000 t_time_sk", "t_time_sk >= 1000")) //列大于临界值的百分比
        .addAnalyzer(Compliance("top 1000 t_time_sk", "t_time_id >= AAAAAAAAOBBFBAAA")) //列大于临界值的百分比
//        .addAnalyzer(Correlation("t_hour", "t_time")) //相关性
//        //        .addAnalyzer(DataType("t_am_pm")) //相关性
//        .addAnalyzer(Distinctness("t_hour")) //列的不同值与列的所有值的比值
//        .addAnalyzer(Entropy("t_hour")) //列的不同值与列的所有值的比值
//        .addAnalyzer(Maximum("t_hour")) //列的不同值与列的所有值的比值
//        .addAnalyzer(Mean("t_hour")) //列的不同值与列的所有值的比值
//        .addAnalyzer(Minimum("t_hour")) //列的不同值与列的所有值的比值
//        .addAnalyzer(MutualInformation(Seq("t_hour", "t_time"))) //列的不同值与列的所有值的比值
//        .addAnalyzer(UniqueValueRatio("t_hour")) //列的不同值与列的所有值的比值
//        .addAnalyzer(Uniqueness("t_hour")) //列的不同值与列的所有值的比值
//        .addAnalyzer(Uniqueness("t_time_id")) //列的不同值与列的所有值的比值
        .run()
    }
    val metrics = successMetricsAsDataFrame(spark, analysisResult);
    metrics.show();
    spark.stop()
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

