package com.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class JoinTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(JoinTest.class);

        SparkConf sparkConf = new SparkConf().setAppName("JoinTest").set("spark.testing.memory", "2147480000").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, String> lessonRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhaoyi", "语文"),
                new Tuple2<>("yuanyong", "数学"),
                new Tuple2<>("zhaoyi", "英语"),
                new Tuple2<>("huangwei", "英语"),
                new Tuple2<>("yuanyong", "法语"),
                new Tuple2<>("xiaozhou", "物理")));
        JavaPairRDD<String, String> lessonRDD2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhaoyi", "日语")));
        final JavaPairRDD<String, Tuple2<String, String>> result = lessonRDD.join(lessonRDD2);
        result.collect().forEach(System.out::println);
        sc.close();
    }

}
