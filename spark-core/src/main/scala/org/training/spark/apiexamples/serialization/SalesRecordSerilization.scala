package org.training.spark.apiexamples.serialization

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by Arjun on 20/1/15.
 */
object SalesRecordSerilization {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    conf.set("spark.logLineage", "true");
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(record => {
      val parseResult = SalesRecordParser.parse(record)
      parseResult.right.get
    })

    val product = salesRecordRDD.map(record => (record.itemId, 1))
    val result = product.reduceByKey(_ + _).sortBy(- _._2)
    println(salesRecordRDD.collect().toList)
    println(product.collect().toList)
  }

}
