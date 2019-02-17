package com.spark.datasource;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceTest1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // spark session
        SparkSession spark = SparkSession.builder().appName("sql").config(sparkConf).getOrCreate();

        Dataset<Row> userDF = spark.read().format("json").load("user.json");


        userDF.show();

        // save df as parquet file.
        userDF.write().format("parquet").save("user.parquet");

        // load parquet file.
        Dataset<Row> user2DF = spark.read().format("parquet").load("user.parquet");
        user2DF.show();


    }
}
