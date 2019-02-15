package com.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class AverageTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(AverageTest.class);

        SparkConf sparkConf = new SparkConf().setAppName("wordCount").set("spark.testing.memory", "2147480000").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Double> scoreRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>("zhaoyi", 90.0),
                new Tuple2<>("huangwei", 95.0),
                new Tuple2<>("yuanyong", 97.0),
                new Tuple2<>("zhaoyi", 100.0),
                new Tuple2<>("huangwei", 100.0),
                new Tuple2<>("yuanyong", 70.0),
                new Tuple2<>("xiaozhou", 96.0)));

        JavaPairRDD<String, Tuple2<Double, Integer>> combineRDD = scoreRDD.combineByKey(score -> new Tuple2<>(score, 1),
                (t, score) -> new Tuple2<>(t._1 + score, t._2 + 1),
                (t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)
        );
        combineRDD.collect().forEach(System.out::println);
        // 计算平均分
        JavaRDD<Tuple2> map = combineRDD.map(t -> new Tuple2(t._1, t._2._1 / t._2._2));

        map.collect().forEach(System.out::println);
        logger.info("success");
        sc.close();
    }
}
