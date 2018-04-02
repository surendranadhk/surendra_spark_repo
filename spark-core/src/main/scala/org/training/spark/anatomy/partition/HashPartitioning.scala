package org.training.spark.anatomy.partition

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 11/3/15.
 */
object HashPartitioning {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("hash partitioning example")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val salesData = sc.textFile(args(1))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1),colValues(2))
    })



    val groupedData = salesByCustomer.groupByKey()

    println("default partitions are "+groupedData.partitions.length)


    //increase partitions
    val groupedDataWithMultiplePartition = salesByCustomer.groupByKey(4)
    println("increased partitions are " + groupedDataWithMultiplePartition.partitions.length)

    println(groupedDataWithMultiplePartition.collect().toList)
  }

}
