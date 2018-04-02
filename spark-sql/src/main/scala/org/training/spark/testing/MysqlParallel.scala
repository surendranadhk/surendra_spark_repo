package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 22/10/16.
 */
object MysqlParallel {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2g")
    val sc: SparkContext = new SparkContext(args(0), "spark_jdbc", sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val mysqlOption = Map("url" -> "jdbc:mysql://hduser:3306/flights",
      "dbtable" -> "ontime",
      "user" -> "hduser",
      "password" -> "hadoop123",
      "fetchSize" -> "10",
      "partitionColumn" -> "Year", "lowerBound" -> "2015", "upperBound" -> "2015", "numPartitions" -> "5")


    val jdbcDF = sqlContext.read
                           .format("org.apache.spark.sql.jdbc")
                           .options(mysqlOption)
                           .load()

    //jdbcDF.printSchema()

    jdbcDF.registerTempTable("ontime")
    

    sqlContext.sql("SELECT Year, Month, Carrier, DepDelay from ontime").write.save(args(1))

    //sqlContext.sql("select Year, Month, count(*)  Total, Avg(DepDelay) Avg from ontime group by Year, Month").show()

  }
}
