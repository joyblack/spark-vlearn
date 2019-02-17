package com.spark.sql;

import com.spark.input.User;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkSqlTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local").set("spark.testing.memory", "2140000000");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("sql").config(conf).getOrCreate();
        // load json file.
        Dataset<Row> rowDataSet = spark.read().json("user.json");

        // print original schema.
        rowDataSet.printSchema();
        rowDataSet.show();



    }
}
