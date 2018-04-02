package org.training.spark.anatomy.partition

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 11/3/15.
 */
object Partitions {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("partition example")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    //actual textFile api converts to the following code
    //val dataRDD = sc.hadoopFile(args(1), classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
    //  sc.defaultMinPartitions).map(pair => pair._2.toString)


    val dataRDD = sc.textFile(args(1), 3)

    println(dataRDD.partitions.length)

    val dataRdd2 = dataRDD.coalesce(1)
    dataRdd2.saveAsTextFile(args(2))
    //dataRDD.repartition(4)

  }


}
