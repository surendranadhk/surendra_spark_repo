package org.training.spark.anatomy.laziness

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 27/3/15.
 */
object RDDDependencies {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RDD dependencies example")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(args(1))


    val flatMapRDD = dataRDD.flatMap(value => value.split(""))

    println("type of flatMapRDD is "+ flatMapRDD.getClass.getSimpleName +" and parent is " + flatMapRDD.dependencies.head.rdd.getClass.getSimpleName)

    val hadoopRDD = dataRDD.dependencies.head.rdd

    println("type of  Data RDD is " +dataRDD.getClass.getSimpleName+"  and the parent is "+ hadoopRDD.getClass.getSimpleName)

    println("parent of hadoopRDD is " + hadoopRDD.dependencies)

    println("RDD graph is" + flatMapRDD.toDebugString)


  }

}
