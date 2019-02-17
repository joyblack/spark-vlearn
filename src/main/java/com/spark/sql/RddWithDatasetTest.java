package com.spark.sql;

import com.spark.input.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;

public class RddWithDatasetTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local").set("spark.testing.memory", "2140000000");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("sql").config(conf).getOrCreate();
        JavaRDD<User> rdd = sc.parallelize(Arrays.asList(new User(1, "zhaoyi"), new User(2, "hongqun"), new User(3,"akuya")));

        // 通过反射获取rdd的元素信息生成df
        Dataset<Row> userDF = spark.createDataFrame(rdd, User.class);

        // 注册临时表
        userDF.createOrReplaceTempView("user");

        // 1. 可以通过索引访问数据列信息，首先需要定义一个解码器
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> nameDF = userDF.map((MapFunction<Row, String>) row -> "Name: " + row.getString(1), stringEncoder);
        nameDF.show();

        // 2. 也可以通过列名称获取列信息
        Dataset<String> nameDF2 = userDF.map((MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"), stringEncoder);
        nameDF2.show();


    }
}
