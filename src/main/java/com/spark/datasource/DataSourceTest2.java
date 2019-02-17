package com.spark.datasource;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSourceTest2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession.builder().appName("sql").config(sparkConf).getOrCreate();

        // sql("select * from parquet.<parquet.path>")
        Dataset<Row> userDF = spark.sql("select * from parquet.`user.parquet`");
        Dataset<Row> userDF2 = spark.sql("select * from json.`user.json`");
        userDF.show();
        userDF2.show();


    }
}
