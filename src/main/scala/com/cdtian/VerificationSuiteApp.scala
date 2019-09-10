package com.cdtian

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.cdtian.DeequApp.Time_Dim
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * @author tianyj3@lenovo.com
  * @date 2019/9/4.
  * @copyright Copyright Lenovo Corporation 2018 All Rights Reserved.
  */
object VerificationSuiteApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("containsEmail 1 isUnique repeat 2").getOrCreate()
    val dataSet = spark.read.textFile("/tmp/tpcds-generate/10/time_dim")
    import spark.implicits._
    val newDataSet = dataSet.map(_.split("\\|")).map(attrs => Time_Dim(attrs(0).toLong, attrs(1), attrs(2).toInt, attrs(3).toInt, attrs(4).toInt, attrs(5).toInt, attrs(6), attrs(7), attrs(8), attrs(8)))(Encoders.product[Time_Dim])
    val dataDf = newDataSet.toDF().cache();
    val verificationResult: VerificationResult = {
      VerificationSuite()
        .onData(dataDf)
        .addCheck(Check(CheckLevel.Error, "review check")
          .hasSize(_ >= 86400)
          .hasMin("t_hour", _ == 0)
          .hasMin("t_hour", _ == 0)
          .hasMean("t_hour", _ == 11.5)
            .hasApproxCountDistinct("t_hour",_ == 25)
            .isContainedIn("t_am_pm",Array("AM","PM"))
              .containsEmail("t_am_pm")
            .isUnique("t_time_sk")
            .isUnique("t_time_sk")
//            .isUnique("t_time_id")
//            .isUnique("t_time")
        )
        .run()
    }
    val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
    resultDataFrame.show(truncate=false)
  }
}
