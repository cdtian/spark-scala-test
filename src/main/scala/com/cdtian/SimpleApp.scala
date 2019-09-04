//package com.cdtian
//
//import com.amazon.deequ.analyzers._
//import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
//import org.apache.spark.sql.SparkSession
//
///**
//  * @author tianyj3@lenovo.com
//  */
//object SimpleApp {
//  def main(args: Array[String]): Unit = {
//    val ss = SparkSession.builder().appName("spark job ").getOrCreate()
//    val data = ss.read.textFile("/tmp/tpcds-generate/10/catalog_returns").cache();
//    //2452481|6047|52545|55368|788973|6578|118583|890|1267613|555|127097|6|8022|3|5|27|1280001|81|1354.32|40.62|1394.94|91.85|1315.44|13.54|925.13|415.65|1447.91|
//    val dataTable = data.map(_.split("|")).map(attributes => Catalog(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5), attributes(6), attributes(7), attributes(8), attributes(9), attributes(10), attributes(11), attributes(12), attributes(13), attributes(14), attributes(15), attributes(16), attributes(17), attributes(19), attributes(20), attributes(21), attributes(22), attributes(23), attributes(24), attributes(25), attributes(26), attributes(27)));
//    dataTable.printSchema();
//    val df = dataTable.toDF();
//    val analysisResult: AnalyzerContext = {
//      AnalysisRunner
//        // data to run the analysis on
//        .onData(df)
//        // define analyzers that compute metrics
//        .addAnalyzer(Completeness("cr_returned_date_sk"))
//        .addAnalyzer(ApproxCountDistinct("cr_returned_time_sk"))
//        .addAnalyzer(Mean("cr_item_sk"))
//        .addAnalyzer(Compliance("top star_rating", "star_rating >= 4.0"))
//        .addAnalyzer(Correlation("total_votes", "star_rating"))
//        .addAnalyzer(Correlation("total_votes", "helpful_votes"))
//        .addAnalyzer(Size()).run()
//
//    }
//  }
//
//  case class Catalog(
//                      cr_returned_date_sk: String,
//                      cr_returned_time_sk: String,
//                      cr_item_sk: String,
//                      cr_refunded_customer_sk: String,
//                      cr_refunded_cdemo_sk: String,
//                      cr_refunded_hdemo_sk: String,
//                      cr_refunded_addr_sk: String,
//                      cr_returning_customer_sk: String,
//                      cr_returning_cdemo_sk: String,
//                      cr_returning_hdemo_sk: String,
//                      cr_returning_addr_sk: String,
//                      cr_call_center_sk: String,
//                      cr_catalog_page_sk: String,
//                      cr_ship_mode_sk: String,
//                      cr_warehouse_sk: String,
//                      cr_reason_sk: String,
//                      cr_order_number: String,
//                      cr_return_quantity: String,
//                      cr_return_amount: String,
//                      cr_return_tax: String,
//                      cr_return_amt_inc_tax: String,
//                      cr_fee: String,
//                      cr_return_ship_cost: String,
//                      cr_refunded_cash: String,
//                      cr_reversed_charge: String,
//                      cr_store_credit: String,
//                      cr_net_loss: String
//                    )
//
//}
