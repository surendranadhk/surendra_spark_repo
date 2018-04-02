package org.training.spark.apiexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by Arjun on 20/1/15.
 */
object ItemWiseCount {


  def getTuple(record: String) ={
    val columns = record.split(",")
    (columns(2), 1)
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("apiexamples")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1), 10)

    //val itemPair = dataRDD.map(getTuple)

      val itemPair = dataRDD.map(record => getTuple(record))

    //val result = itemPair.reduceByKey((x, y) => x + y)
    val resultRdd = itemPair.reduceByKey(_+_).sortBy(- _._2)    //  - indicates desinding order

    //resultRdd.saveAsTextFile("output/path")
    println(resultRdd.collect().toList)

  }
}
