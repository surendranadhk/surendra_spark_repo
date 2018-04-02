package org.training.spark.anatomy.partition

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 11/3/15.
 */
object LookUp {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("look up example")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val salesData = sc.textFile(args(1))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1), colValues(3).toDouble)
    })


    val groupedData = salesByCustomer.groupByKey(new CustomerPartitioner(2))

/*
    val groupedDataWithPartitionData = groupedData.mapPartitionsWithIndex((partitionNo, iterator) => {
      println(" accessed partition " + partitionNo)
      iterator
    }, true)*/


    println("for accessing customer id 1")
    groupedData.lookup("1")
    //groupedDataWithPartitionData.lookup("1")


  }


}
