package org.training.spark.hadoopintegration.read

import org.training.spark.hadoopintegration.SalesRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 20/1/15.
 */
object SequenceFileRead {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("hadoopintegration")
    val sc = new SparkContext(conf)

    val dataRDD = sc.sequenceFile(args(1),classOf[NullWritable],classOf[SalesRecordWritable]).map(_._2)
    val ItemPair = dataRDD.map(x => (x.getItemId, 1))
    val ItemWiseCount = ItemPair.reduceByKey(_ + _)
    println(ItemWiseCount.collect().toList)

  }

}
