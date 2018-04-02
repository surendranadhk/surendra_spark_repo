package org.training.spark.anatomy.partition

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 11/3/15.
 */
object MapPartitions {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("map partition example")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val salesData = sc.textFile(args(1))

    val (itemMin,itemMax)=salesData.mapPartitions(partitionItertor => {

      val (min,max) = partitionItertor.foldLeft((Double.MaxValue,Double.MinValue))((acc,salesRecord) => {
        val itemValue = salesRecord.split(",")(3).toDouble
        (acc._1 min itemValue , acc._2 max itemValue)
      })
      println("min" + min + "max " + max)
      List((min,max)).iterator
    }).reduce((a,b)=> (a._1 min b._1 , a._2 max b._2))
    println("min = "+itemMin + " max ="+itemMax)

  }

}
