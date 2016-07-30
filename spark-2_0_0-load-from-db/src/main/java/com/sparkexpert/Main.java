package com.sparkexpert;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class Main implements Serializable {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Main.class);

    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/employees";
    private static final String MYSQL_USERNAME = "expertuser";
    private static final String MYSQL_PWD = "expertuser123";

    private static final JavaSparkContext sc =
            new JavaSparkContext(new SparkConf().setAppName("Spark2JdbcDs").setMaster("local[*]"));

    private static final SQLContext sqlContext = new SQLContext(sc);

    public static void main(String[] args) {
        //JDBC connection properties
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);

        final String dbTable =
                "(select emp_no, concat_ws(' ', first_name, last_name) as full_name from employees) as employees_name";

        //Load MySQL query result as Dataset
        Dataset<Row> jdbcDF =
                sqlContext.read()
                        .jdbc(MYSQL_CONNECTION_URL, dbTable, "emp_no", 10001, 499999, 10, connectionProperties);

        List<Row> employeeFullNameRows = jdbcDF.collectAsList();

        for (Row employeeFullNameRow : employeeFullNameRows) {
            LOGGER.info(employeeFullNameRow);
        }
    }
}
