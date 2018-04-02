package org.training.spark.testing.xml

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hduser on 13/8/16.
 */
object Ages {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "xml file", sparkConf)
    val sqlContext = new SQLContext(sc)
    val xmlOptions = Map("rowTag" -> "person")
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .options(xmlOptions)
      .load(args(1)).cache()
    df.printSchema()
    df.show



    df.select("age.#VALUE", "age.@born").show()

    //rename the column
    import sqlContext.implicits._
    //df.select($"age.#VALUE".alias("acturalAge") , $"age.@born".alias("birthdate")).show()
    val df1 = df.select($"age.#VALUE" alias "actualAge" , $"age.@born" alias "birthdate").where("actualAge > 25")

    df.selectExpr("age as actualAge").show()

    df1.registerTempTable("Ages")
    sqlContext.sql("select actualAge,count(actualAge) from ages group by actualAge")
  }
}
