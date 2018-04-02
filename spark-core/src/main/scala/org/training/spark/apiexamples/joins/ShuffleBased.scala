package org.training.spark.apiexamples.joins

import org.training.spark.apiexamples.serialization.SalesRecordParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by Arjun on 20/1/15.
 */
object ShuffleBased {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)

    val salesRDD = sc.textFile(args(1))
    val customerRDD = sc.textFile(args(2))

    val salesPair = salesRDD.map(row => {
      val salesRecord = SalesRecordParser.parse(row).right.get
      (salesRecord.customerId,salesRecord)
    })

    val customerPair = customerRDD.map(row => {
      val columnValues = row.split(",")
      (columnValues(0),columnValues(1))
    })
//println(salesPair.collect().toList)
//println(customerPair.collect().toList)

    val joinRDD = customerPair.join(salesPair)

    println(joinRDD.collect().toList)
      val result = joinRDD.map{
      case (customerId,(customerName,salesRecord)) => {
        (customerName,salesRecord.itemId)
      }
    }

    println(result.collect().toList) // Not recommended to use colllect after join....

  }


}
