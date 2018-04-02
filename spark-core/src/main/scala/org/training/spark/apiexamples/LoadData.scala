package org.training.spark.apiexamples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 20/1/15.
 */
object LoadData {

  def main(args: Array[String]) {
    //create spark context

    val master = args(0)
    val path = args(1)

    val conf = new SparkConf().setAppName("laoddata")
    conf.setMaster(master)
    val sc = new SparkContext(conf)

    //it creates RDD[String] of type MappedRDD

    //val x = sc.parallelize(List(1, 2, 3, 4))  //Memory

    val dataRDD = sc.textFile(path)



    //print the content . Converting to List just for nice formatting
    println(dataRDD.collect().toList)

  }

}
