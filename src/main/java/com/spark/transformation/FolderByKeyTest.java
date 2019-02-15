package com.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class FolderByKeyTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(FolderByKeyTest.class);

        SparkConf sparkConf = new SparkConf().setAppName("AggregateTest").set("spark.testing.memory", "2147480000").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Double> scoreRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>("zhaoyi", 90.0),
                new Tuple2<>("huangwei", 95.0),
                new Tuple2<>("yuanyong", 97.0),
                new Tuple2<>("zhaoyi", 100.0),
                new Tuple2<>("huangwei", 100.0),
                new Tuple2<>("yuanyong", 70.0),
                new Tuple2<>("xiaozhou", 96.0)));

        JavaPairRDD<String, Double> stringDoubleJavaPairRDD = scoreRDD.foldByKey(0D,
                (a, b) -> Math.max(a, b));
        stringDoubleJavaPairRDD.collect().forEach(System.out::println);
        sc.close();
    }

    public static class SortByKeyTest {
        public static void main(String[] args) {
            Logger logger = LoggerFactory.getLogger(SortByKeyTest.class);

            SparkConf sparkConf = new SparkConf().setAppName("AggregateTest").set("spark.testing.memory", "2147480000").setMaster("local");

            JavaSparkContext sc = new JavaSparkContext(sparkConf);

            JavaPairRDD<String, Double> scoreRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>("zhaoyi", 90.0),
                    new Tuple2<>("huangwei", 95.0),
                    new Tuple2<>("yuanyong", 97.0),
                    new Tuple2<>("zhaoyi", 100.0),
                    new Tuple2<>("huangwei", 100.0),
                    new Tuple2<>("yuanyong", 70.0),
                    new Tuple2<>("xiaozhou", 96.0)));
            JavaPairRDD<String, Double> stringDoubleJavaPairRDD = scoreRDD.sortByKey(true);
            stringDoubleJavaPairRDD.collect().forEach(System.out::println);
            sc.close();
        }
    }
}
