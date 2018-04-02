package org.training.spark.testing

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 26/8/16.
 */

case class Employee(firstName: String, lastName: String, email: String)
case class Department(id: String, name: String)
case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])



object EmployeeExplode {



  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc  = new SparkContext(args(0), "csvfile", conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)



    val employee1 = new Employee("michael", "armbrust", "abc123@prodigy.net")
    val employee2 = new Employee("chris", "fregly", "def456@compuserve.net")

    val department1 = new Department("123456", "Engineering")
    val department2 = new Department("123456", "Psychology")
    val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
    val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee1, employee2))

    import sqlContext.implicits._
    val departmentWithEmployeesDF = sc.parallelize(Seq(departmentWithEmployees1, departmentWithEmployees2)).toDF()

    departmentWithEmployeesDF.printSchema()

    val explodedDepartmentWithEmployeesDF = departmentWithEmployeesDF.explode(departmentWithEmployeesDF("employees")) {
      case Row(employee: Seq[Row]) => employee.map(employee =>
        Employee(employee(0).asInstanceOf[String], employee(1).asInstanceOf[String], employee(2).asInstanceOf[String])
      )
    }
    explodedDepartmentWithEmployeesDF.printSchema()
    explodedDepartmentWithEmployeesDF.show()


  }
}
