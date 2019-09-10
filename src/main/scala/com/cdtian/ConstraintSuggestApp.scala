package com.cdtian

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql.{Encoders, SparkSession}
import com.cdtian.DeequApp.Time_Dim

/**
  * @author tianyj3@lenovo.com
  * @date 2019/9/4.
  * @copyright Copyright Lenovo Corporation 2018 All Rights Reserved.
  */
object ConstraintSuggestApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Deequ verification suit").getOrCreate()
    val dataSet = spark.read.textFile("/tmp/tpcds-generate/10/time_dim")
    import spark.implicits._
    val newDataSet = dataSet.map(_.split("\\|")).map(attrs => Time_Dim(attrs(0).toLong, attrs(1), attrs(2).toInt, attrs(3).toInt, attrs(4).toInt, attrs(5).toInt, attrs(6), attrs(7), attrs(8), attrs(8)))(Encoders.product[Time_Dim])
    val dataDf = newDataSet.toDF().cache();
    val suggestionResult = {
      ConstraintSuggestionRunner()
        .onData(dataDf)
        .addConstraintRules(Rules.DEFAULT).run()
    }
    val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap {
      case (column, suggestions) => suggestions.map {
        constraint => (column, constraint.description, constraint.codeForConstraint)
      }
    }.toSeq.toDS()

    suggestionDataFrame.createOrReplaceTempView("suggest")
  }
}
