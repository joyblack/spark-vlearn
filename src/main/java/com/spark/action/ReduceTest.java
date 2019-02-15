package com.spark.action;

import com.spark.transformation.AggregateTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class ReduceTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Double> scoreRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>("zhaoyi", 90.0),
                new Tuple2<>("huangwei", 95.0),
                new Tuple2<>("yuanyong", 97.0),
                new Tuple2<>("zhaoyi", 100.0),
                new Tuple2<>("huangwei", 100.0),
                new Tuple2<>("yuanyong", 70.0),
                new Tuple2<>("xiaozhou", 96.0)));
        Tuple2<String, Double> result = scoreRDD.reduce((lession1, lesstion2) -> new Tuple2<>("result", lession1._2 + lesstion2._2));
        System.out.println(result);
    }
}
