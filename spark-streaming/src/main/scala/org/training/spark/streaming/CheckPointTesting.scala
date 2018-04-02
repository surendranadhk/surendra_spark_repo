package org.training.spark.streaming

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by hduser on 11/3/16.
 */
object CheckPointTesting {

  def updateFunction(values: Seq[Int], runningCount: Option[Int]) = {
    val newCount = values.sum + runningCount.getOrElse(0)
    new Some(newCount)
  }

  def main(args: Array[String]) {

    //val checkPointDirectory = "hdfs://localhost:9000/user/hduser/checkpoint"
    val master = args(0)
    val ipaddr = args(1)
    val port = args(2).toInt
    val checkPointDirectoryPath = args(3)

    //val sc = new SparkContext("local", "testing")

    val ssc = StreamingContext.getOrCreate(checkPointDirectoryPath,() => {
      println("new context getting started")
      val conf = new SparkConf().setAppName("RecoverableWordCount").setMaster(master)
      //val context = new StreamingContext(args(0), "RecoverableWordCount", Seconds(3))
      val context = new StreamingContext(conf, Seconds(5))
      context.checkpoint(checkPointDirectoryPath)

      conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
      val lines = context.socketTextStream(ipaddr,port)

      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)
      val totalWordCount = wordCounts.updateStateByKey(updateFunction _)
      //totalWordCount.cache
      //totalWordCount.checkpoint(Seconds(25))
      totalWordCount.print()


      context
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
