package org.training.spark.hadoopintegration.write

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Arjun on 20/1/15.
 */
object TextFilePersist {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("hadoopintegration")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))
    val outputPath = args(2)
    val itemPair = dataRDD.map(row => {
      val columns = row.split(",")
      (columns(2), 1)
    })
    /*
    itemPair is MappedRDD which is a pair. We can import the following to get more methods

     */
    import org.apache.spark.SparkContext._

    //val result1 = itemPair.reduceByKey(_+_).map(item => item.swap).sortByKey(false)
    val result = itemPair.reduceByKey(_+_).sortBy(- _._2)

    result.saveAsTextFile(outputPath)


  }

}
