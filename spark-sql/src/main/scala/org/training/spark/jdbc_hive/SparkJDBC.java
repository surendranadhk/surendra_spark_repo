package org.training.spark.jdbc_hive;

import java.sql.*;

/**
 * Created by hduser on 15/2/15.
 */
public class SparkJDBC {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection cnct = DriverManager.getConnection("jdbc:hive2://localhost:10000", "hduser", "training");

        Statement stmt = cnct.createStatement();
        stmt.execute("create table sales_jdbc(transactionId string, customerId String, itemId string, amount double) row format delimited fields terminated by \",\" stored as textfile");
        stmt.execute("load data local inpath '"+args[0]+"' overwrite into table sales_jdbc");
        ResultSet rset = stmt.executeQuery("SELECT * FROM sales_jdbc");

        while (rset.next()) {
            System.out.print(rset.getInt(1)+"  ");
            System.out.print(rset.getInt(2));
            System.out.println();
        }

        stmt.execute("drop table sales_jdbc");

    }
}
