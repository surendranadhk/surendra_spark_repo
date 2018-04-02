package org.training.spark.hiveintegration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by hduser on 7/9/15.
 */
object HiveUDF {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("hiveudf")
    val sc: SparkContext = new SparkContext(conf)

    /*
    System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:mysql://hduser/metastore?createDatabaseIfNotExist=true")


    System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")

    System.setProperty("javax.jdo.option.ConnectionUserName", "hduser")

    System.setProperty("javax.jdo.option.ConnectionPassword", "training")  */

    val hiveContext = new HiveContext(sc)
    val customers = hiveContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
    customers.registerTempTable("customers")
    hiveContext.sql("create temporary function custom_lower as 'org.training.spark.hiveintegration.Lower'")
    hiveContext.sql("select custom_lower(name) from customers").show()
  }

}
