package org.spark.training.apiexamples

import org.apache.spark.sql.SparkSession

/**
  * Created by hduser on 6/5/16.
  */
object DataSetWordCount {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("src/main/resources/data.txt").as[String]

    val words = data.flatMap(value => value.split("\\s+"))
   // words.
    //val groupedWords = words.groupByKey(_.toLowerCase)
    val groupedWords = words.groupByKey(_.toLowerCase)

    println(groupedWords.keys)

    val counts = groupedWords.count()

    counts.show()


  }

}
