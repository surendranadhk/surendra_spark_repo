package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 12/4/16.
 */

case class SimpleIdValue(id: Int, value: String, desc: String)

object GroupByTesting {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("grouping")
    val sc  = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val data = sc.parallelize(Seq((1, "v1", "d1"), (1, "v2", "d2"), (2, "v21", "d21"), (2, "v22", "d22")))
                 .toDF("id", "value", "desc")

    val data1 =sc.parallelize(Seq((1, "v1", "d1"), (1, "v2", "d2"), (2, "v21", "d21"), (2, "v22", "d22")))

    val data2 = data1.map(line => SimpleIdValue(line._1, line._2, line._3))

    val data1DF = sqlContext.createDataFrame(data1)

    data.registerTempTable("data")
    val result = sqlContext.sql("select id,concat_ws(';', collect_list(value)),concat_ws(';', collect_list(value)) from data group by id")
    result.show
  }
}
