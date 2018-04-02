package org.training.spark.hadoopintegration.read

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 20/1/15.
 */
object TextFileRead {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("hadoopintegration")
    val sc = new SparkContext(conf)

    val dataRDD1 = sc.textFile(args(1))
    //actual textFile api converts to the following code
    val dataRDD = sc.hadoopFile(args(1), classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      sc.defaultMinPartitions).map(pair => pair._2.toString)

    println(dataRDD.collect().toList)

  }


}
