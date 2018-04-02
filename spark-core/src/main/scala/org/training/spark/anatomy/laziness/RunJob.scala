package org.training.spark.anatomy.laziness

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 24/3/15.
 */
object RunJob {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("run job example")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val salesData = sc.textFile(args(1))

    println("built in collect " +salesData.collect().toList)
    //implement collect using runJob API

    val results = sc.runJob(salesData, (iter: Iterator[String]) => iter.toArray)
    val collectedResult = Array.concat(results: _*).toList
    println("result of custom collect " +collectedResult)


  }

}
