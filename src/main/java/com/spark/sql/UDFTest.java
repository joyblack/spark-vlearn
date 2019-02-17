package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UDFTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local").set("spark.testing.memory", "2140000000");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("sql").config(conf).getOrCreate();

        // Register the function to access it
        spark.udf().register("myAverage", new MyAverage());
        Dataset<Row> df = spark.read().json("employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(score) as average_salary FROM employees");
        result.show();
    }
}
