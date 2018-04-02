package org.training.spark.anatomy.partition

import org.training.spark.anatomy.partition.CustomerPartitioner
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 11/3/15.
 */
object CustomPartition {


  
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("custom partitioning example")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val salesData = sc.textFile(args(1))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1),colValues(2))
    })

    val groupedData = salesByCustomer.groupByKey(new CustomerPartitioner(2))

    //printing partition specific data

    val groupedDataWithPartitionData = groupedData.mapPartitionsWithIndex{
      case (partitionNo,iterator) => {
       List((partitionNo,iterator.toList.length)).iterator
      }
    }

    println(groupedDataWithPartitionData.collect().toList)
    
    
    
  }
  

}
