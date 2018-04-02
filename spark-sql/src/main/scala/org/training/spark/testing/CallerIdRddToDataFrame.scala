package org.training.spark.testing

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 29/3/16.
 */


case class CallRecord(callId: String, ocallId: String, callTime: String, duration: String, calltype: String, swId: String)

object CallerIdRddToDataFrame {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("CalledId Example")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val a =
      Array(
        Array("4580056797", "0", "2015-07-29 10:38:42", "0", "1", "1"),
        Array("4580056797", "0", "2015-07-29 10:38:42", "0", "1", "1"))

    val callRecordRdd = sc.makeRDD(a)
    val callRecordDF = callRecordRdd.map{case Array(s0, s1, s2, s3, s4, s5) => CallRecord(s0, s1, s2, s3, s4, s5)}.toDF
    callRecordDF.printSchema()
    callRecordDF.show()
  }
}
