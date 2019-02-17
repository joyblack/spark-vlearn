package com.spark.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class HiveTableTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local").set("spark.testing.memory", "2140000000");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // warehouseLocation指向托管数据库和表的默认位置
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        // spark配置
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.testing.memory", "2147480000")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("create database if not exists spark_hive");

        Dataset<Row> show_databases = spark.sql("show databases");

        show_databases.show();


        spark.sql("use spark_hive");

        spark.sql("create table if not exists `tab_name`(id int,name string)");
        spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");
        System.out.println("success...");





    }
}
