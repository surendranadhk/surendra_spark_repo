package org.training.spark.apiexamples.advanced

import org.training.spark.apiexamples.serialization.SalesRecordParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by madhu on 28/1/15.
 */
object joldByKey {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("apiexample")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val byCustomer = salesRecordRDD.map(salesRecord => (salesRecord.customerId,salesRecord.itemValue))
    val maxByCustomer = byCustomer.foldByKey(Double.MinValue)((acc,itemValue) => {
      if(itemValue > acc ) itemValue else acc
    })
    val sortedByValue = maxByCustomer.sortBy(- _._2)
    println(sortedByValue.collect().toList)



  }

}
