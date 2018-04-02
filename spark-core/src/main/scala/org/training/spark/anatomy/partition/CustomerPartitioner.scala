package org.training.spark.anatomy.partition

import org.apache.spark.Partitioner

/**
 * Created by Arjun on 11/3/15.
 */
class CustomerPartitioner(np: Int) extends Partitioner {
  
  def numPartitions: Int = np

  def getPartition(key: Any): Int =
    key match {
      case null => 0
      case _ => {
        val keyValue = key.toString.toInt
        if(keyValue>=2) 1 else 0
      }
    }
}
