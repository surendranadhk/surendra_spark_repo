package org.training.spark.testing.xml

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by hduser on 11/28/16.
  */
object XmlFlatten extends App {

  val conf = new SparkConf().setMaster("local").setAppName("Xml flatten job")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val xmlOptions = Map("rowTag" -> "catalog_item")

  val xmlDF = sqlContext.read.format("com.databricks.spark.xml").options(xmlOptions).load("src/main/resources/XMLInput.xml")

  xmlDF.printSchema()
  xmlDF.show()

  val xmlDF1 = xmlDF.select(col("@gender").alias("gender"), col("item_number"), col("price"), explode(col("size.@description")).alias("size_value"), col("size"))//withColumn("size", explode(col("size.@description"))).show

  val xmlDF2 = xmlDF1.withColumn("size_color", explode(col("size.color_swatch")))

  val xmlDF3 = xmlDF2.withColumn("size_color", explode(col("size_color"))).select(col("gender"), col("item_number"), col("price"), col("size_value"), col("size_color.#VALUE").alias("size_color")).dropDuplicates()

  //xmlDF3.select("size_color.#VALUE").show()
  xmlDF3.printSchema()
  xmlDF3.show()
}
