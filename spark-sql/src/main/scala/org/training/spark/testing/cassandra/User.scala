package org.training.spark.testing.cassandra

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkContext,  SparkConf}

/**
 * Created by hduser on 28/6/16.
 */
object User {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()

    val master =  args(0)
    sparkConf.setMaster(master)
    sparkConf.setAppName("Userdata")

    sparkConf.set("spark.cassandra.connection.host", "localhost")
    sparkConf.set("spark.cassandra.connection.keep_alive_ms", "40000")

    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val cassandraOptions = Map( "table" -> "sales", "keyspace" -> "ecommerce")
    /*val usersdf = sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options(cassandraOptions)
                         .load()*/

    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").
      option("inferSchema", "true").load(args(1))
   //usersdf.registerTempTable("users")
    //val adults = sqlContext.sql("Select * from users where age > 21").show()
    //usersdf.select("age", "user_id", "name").show()

    salesDf.select(salesDf("customerId").alias("customerid"), salesDf("itemId").alias("itemid")).write
        .format("org.apache.spark.sql.cassandra")
        .options(cassandraOptions).mode(SaveMode.Overwrite).save()

    /* Cassandra commands
    create keyspace ecommerce with replication = { 'class':'SimpleStrategy', 'replication_factor':1} ;
    create table ecommerce.sales(customerId int, itemId string);
     */
  }
}
