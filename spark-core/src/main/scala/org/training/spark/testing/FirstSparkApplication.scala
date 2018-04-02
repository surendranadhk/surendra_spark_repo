package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 6/13/17.
  */
object FirstSparkApplication extends App{
  val conf=new SparkConf().setMaster("local")
    .setAppName("Fist Spak Application")
  val sc=new SparkContext(conf)
  val rdd1=sc.textFile("/home/hduser/Projects/spark-core/src/main/resources/sales.csv")
  println(rdd1.collect().toList)

}
