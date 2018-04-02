package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.coalesce

/**
 * Created by hduser on 29/3/16.
 */
object FullJoin {

  def main(args: Array[String]) {


    /* ******If keys match, get values from Right side.
      If key on the left side doesn't exist on Right. Take values from left.
    If key on the right side doesn't exist on Left. Take values from right ******/

    val conf = new SparkConf().setMaster(args(0)).setAppName("fullJoin")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val dfLeft = sc.parallelize(Seq((2, "2L"), (3, "3L"))).toDF("kl", "vl")
    dfLeft.show

    val dfRight = sc.parallelize(Seq((1, "1R"), (3, "3R"))).toDF("kr", "vr")
    dfRight.show

    dfLeft
      .join(dfRight, $"kl" === $"kr", "fullouter")
      .select(coalesce($"kl", $"kr").alias("k"), coalesce($"vr", $"vl").alias("v"))
      .show
  }
}
