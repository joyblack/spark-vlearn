package com.spark.input;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFileInputTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(com.spark.transformation.AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 1.加载文件，按行读取
        JavaRDD<String> rdd1 = sc.textFile("input");
        // 2.加载文件,行文件读取
        JavaPairRDD<String, String> rdd2 = sc.wholeTextFiles("input");
        // 存储
        rdd1.saveAsTextFile("out1");
        rdd2.saveAsTextFile("out2");


    }
}
