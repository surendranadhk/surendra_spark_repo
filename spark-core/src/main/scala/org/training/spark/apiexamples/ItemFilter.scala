package org.training.spark.apiexamples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 20/1/15.
 */
object ItemFilter {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("apiexamples")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))
    val itemID1 = args(2)
    val itemID2 = args(3)

    val itemRecords =  dataRDD.filter(record =>{
      val columns = record.split(",")
      val itemId = columns(2)
      if(itemId.equals(itemID1) || itemId == itemID2) true
      else false
    })



/*
    val itemRecords = dataRDD.map(x=>x.split(","))
      .map(colarr=>(colarr(0),colarr(2)))
      .filter(tuple._2.equals(itemID1) || tuple._2.equals(itemID2)) true
    else false*/

    println(itemRecords.collect().toList)
  }
}
