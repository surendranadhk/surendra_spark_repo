package org.training.spark.anatomy

import org.apache.spark.storage.RDDBlockId
import org.apache.spark.{Partition, SparkConf, SparkContext, SparkEnv}

/**
 * Created by Arjun on 24/3/15.
 */
object CacheExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("cache example")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)
    val salesData = sc.textFile(args(1),2)

    val salesByCustomer = salesData.map(value => {
      println("computed")
      val colValues = value.split(",")
      (colValues(1), colValues(3).toDouble)
    })

    println("salesData rdd id is " + salesData.id)
    println("salesBy customer id is " + salesByCustomer.id)



    val firstPartition = salesData.partitions.head

    salesData.cache()

    println(" the persisted RDD's " + sc.getPersistentRDDs)

    //check whether its in cache
    val blockManager = SparkEnv.get.blockManager
    val key = RDDBlockId(salesData.id, firstPartition.index)

    println("before evaluation " +blockManager.get(key))

    // after execution

    salesData.count()


    println("after evaluation " +blockManager.get(key))






















  }


}
