package org.training.spark.testing.Invoice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame

/**
 * Created by hduser on 11/7/16.
 */
object PivotTesting {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("pivot table testing")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    import sqlContext.implicits._
    val df = sc.parallelize(Seq(
      (1, "US", 50),
      (1, "UK", 100),
      (1, "CAN", 125),
      (2, "US", 75),
      (2, "UK", 150),
      (2, "CAN", 175)
    )).toDF("id", "tag", "value")


    //val countries = df.select("tag").distinct().map(_.getString(0)).collect.toList

    val countries = List("US", "UK", "CAN")

    val numCountries = countries.length - 1

    var query = "select *, "
    for (i <- 0 to numCountries-1) {
      query += """case when tag = """" + countries(i) + """" then value else 0 end as """ + countries(i) + ", "

    }
    query += """case when tag = """" + countries.last + """" then value else 0 end as """ + countries.last + " from myTable"


    println(query)


    df.registerTempTable("myTable")
    val myDataFrame = sqlContext.sql(query)
    myDataFrame.registerTempTable("pivotedTable")
    myDataFrame.printSchema()
    sqlContext.sql("SELECT * from pivotedTable").show()
    sqlContext.sql("select id, sum(US) US, sum(UK) UK, sum(CAN) CAN  from pivotedTable group by id").show()
  }
}
