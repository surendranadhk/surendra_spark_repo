package org.training.spark.apiexamples.serialization

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 8/15/17.
  */
object WordCount extends App{

  val conf=new SparkConf().setAppName("word count").setMaster(args(0))
  val sc =new SparkContext(conf)
  val inputRDD=sc.textFile(args(1))
  val wordRDD=inputRDD.flatMap(rec=>rec.split(" "))
  val wordPairRdd=wordRDD.map(word=>(word,1))
  val wordCountRdd=wordPairRdd.reduceByKey(_+_)

  println(wordCountRdd.collect().toList)

}
