package com.sparkexpert;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class Main implements Serializable {

    private static final String MYSQL_USERNAME = "expertuser";
    private static final String MYSQL_PWD = "expertuser123";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mysql://localhost:3306/employees?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;

    private static final JavaSparkContext sc =
            new JavaSparkContext(new SparkConf().setAppName("SparkSaveToDb").setMaster("local[*]"));

    private static final SQLContext sqlContext = new SQLContext(sc);

    public static void main(String[] args) {
        //Sample data-frame loaded from a JSON file
        DataFrame usersDf = sqlContext.jsonFile("src/main/resources/users.json");

        //Save data-frame to MySQL (or any other JDBC supported databases)
        //Choose one of 2 options depending on your requirement (Not both).

        //Option 1: Create new table and insert all records.
        usersDf.createJDBCTable(MYSQL_CONNECTION_URL, "users", true);

        //Option 2: Insert all records to an existing table.
        usersDf.insertIntoJDBC(MYSQL_CONNECTION_URL, "users", false);
    }
}
