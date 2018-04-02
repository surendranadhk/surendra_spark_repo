package org.training.spark.testing.xml

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hduser on 22/8/16.
 */
object EmployeeLogin {

  def main(args: Array[String]) {


    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "EmployeedLogin", sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //val opt = Map("header" -> "true", "InferSchema" -> "true", "delimiter" -> "|")
    val opt = Map("header" -> "true", "InferSchema" -> "true")


    val employeedf = sqlContext.read
      .format("com.databricks.spark.csv")
      .options(opt)
      .load(args(1))

    employeedf.show()
    employeedf.registerTempTable("employees")
    sqlContext.cacheTable("employees")
    //sqlContext.sql("select ID from employees m1 where 0 < (select count(*) total_count from employees m2 where m2.ID = m1.ID - 1 and m2.ColA = m1.ColA and m2.ColB = m1.ColB)").show()
    //sqlContext.sql("select  from  (select count(*) total_count from employees m2 where m2.ID = m1.ID - 1 and m2.ColA = m1.ColA and m2.ColB = m1.ColB)").show()

    val duplicatedRecordsdf = sqlContext.sql("select e1.ID from employees e1 join employees e2 on e1.ID=e2.ID - 1 where e2.ColA=e1.ColA")
    duplicatedRecordsdf.registerTempTable("duplicates")
    sqlContext.cacheTable("duplicates")
    sqlContext.sql("select * from duplicates").show()

    val goodemprecords = sqlContext.sql("select * from employees  left outer join duplicates on employees.ID=duplicates.ID").show()
    //sqlContext.sql("delete ")


  }
}
