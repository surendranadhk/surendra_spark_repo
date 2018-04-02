package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 14/5/16.
 */
object HiveRead {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext(args(0), "CreateHiveTable", conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    System.setProperty("javax.jdo.option.ConnectionURL",
      "jdbc:mysql://localhost/hive_metastore?createDatabaseIfNotExist=true")
    System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
    System.setProperty("javax.jdo.option.ConnectionUserName", "root")
    System.setProperty("javax.jdo.option.ConnectionPassword", "training")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")

    val hqlContext = new org.apache.spark.sql.hive.HiveContext(sc)


            //System.setProperty("hive.metastore.warehouse.dir", "hdfs://hduser9000/user/hive/warehouse")


            hqlContext.sql("show tables").show()

            //hqlContext.read.format("textFile").load("hdfs://localhost:9000/user/hive/warehouse/salestable")
            val hiveData = hqlContext.sql("select * from salestable limit 10").show()
            //val hiveData = hqlContext.sql("select * from commerce_logs limit 10").show()

            //val commerceData = hqlContext.read.format("csv").load("hdfs://localhost:9000/user/hduser/data/ecommerce/2014/")
    //val peopleDF = hqlContext.read.format("orc").load("hdfs://localhost:9000/user/hive/warehouse/people1")
    //peopleDF.printSchema()
    //peopleDF.registerTempTable("people")



  }
}
