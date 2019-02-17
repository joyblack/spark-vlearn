package com.spark.sql;

import com.spark.input.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkSqlTest2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local").set("spark.testing.memory", "2140000000");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("sql").config(conf).getOrCreate();
        JavaRDD<User> rdd = sc.parallelize(Arrays.asList(new User(1, "zhaoyi"), new User(2, "hongqun"), new User(3,"akuya")));
        Dataset<Row> userDataset = spark.createDataFrame(rdd, User.class);
        // Register the DataFrame as a SQL temporary view
        userDataset.createOrReplaceTempView("user");
        Dataset<Row> firstUserDataset = spark.sql("select * from user where id = 1");
        firstUserDataset.show();
    }
}
