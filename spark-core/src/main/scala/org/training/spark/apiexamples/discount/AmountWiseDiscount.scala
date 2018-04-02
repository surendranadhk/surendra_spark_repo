package org.training.spark.apiexamples.discount

import org.training.spark.apiexamples.serialization.{SalesRecord, SalesRecordParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by madhu on 20/1/15.
 */
object AmountWiseDiscount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))

    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val totalAmountByCustomer = salesRecordRDD.map(record => (record.customerId, record.itemValue))
                                              .reduceByKey(_ + _)
/*
   // (custId, TotalAmount)
    val discountAmountByCustomer = totalAmountByCustomer.map(record => {
            val custId = record._1
            val totalAmt = record._2
          if(totalAmt > 1600) {
            val afterDiscount = totalAmt - (totalAmt * 10) / 100.0
            (custId, afterDiscount)
          }
          else
            (custId, totalAmt)

   }) */


    val discountAmountByCustomer = totalAmountByCustomer.map {
      case (customerId, totalAmount) => {
        if (totalAmount > 1600) {
          val afterDiscount = totalAmount - (totalAmount * 10) / 100.0
          (customerId, afterDiscount)
        }
        else (customerId, totalAmount)
      }
    }

    println(discountAmountByCustomer.collect().toList)


  }

}
