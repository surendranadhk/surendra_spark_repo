/**
  * Created by sumantht on 11/8/2016.
  */
import org.apache.spark.{SparkConf, SparkContext}

object wordcount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)

    val l = List("Learning Scala, Learning Spark, Running Spark with Scala", "Hello Scala", "Hello Spark")
    val rdd = sc.parallelize(l)

    rdd.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_).foreach(println)
  }

}
