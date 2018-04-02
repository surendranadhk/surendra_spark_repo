package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by hduser on 13/4/16.
 */
object GroupAggregate {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("groupBy")
    val sc  = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val data = sc.parallelize(Seq((1, "v1", "d1"), (1, "v2", "d2"), (2, "v21", "d21"), (2, "v22", "d22")))
      .toDF("id", "value", "desc")

    data.groupBy("id").agg(("value", "count")).show()
  }
}
