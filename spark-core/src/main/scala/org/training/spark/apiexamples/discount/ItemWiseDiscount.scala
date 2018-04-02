package org.training.spark.apiexamples.discount

import org.training.spark.apiexamples.serialization.{SalesRecord, SalesRecordParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by Arjun on 20/1/15.
 */
object ItemWiseDiscount {





  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")

    //following 2 lines are required for spark history server. All DAGs are shown in UI
    //conf.set("spark.eventLog.enabled", "true")
    //conf.set("spark.eventLog.dir", "/tmp/spark-events")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))
    //val dataRDD = sc.textFile("hdfs://localhost:9000/user/hduser/data/sales/sales.csv")
    val salesRecordRDD = dataRDD.map(record => {
      val parseResult = SalesRecordParser.parse(record)
          parseResult.right.get
    })
   //salesRecordRDD.sortBy(- _.itemValue)


    //apply discount for each book from a customer

    val itemDiscountRDD = salesRecordRDD.map(sales => {
      val itemValue = sales.itemValue
      val newItemValue = itemValue - (itemValue * 5) / 100.0
      (sales.customerId, newItemValue)
    })


    val totalAmountByCustomer = itemDiscountRDD.reduceByKey(_+_).sortBy(- _._2)
    //val totalAmountByCustomer = itemDiscountRDD.map(row => (row.customerId,row.itemValue)).reduceByKey(_+_)
    println(totalAmountByCustomer.collect().toList)

  }

}
