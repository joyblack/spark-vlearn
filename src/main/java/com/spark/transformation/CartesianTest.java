package com.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class CartesianTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(CartesianTest.class);

        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(4,5,6));
        JavaPairRDD<Integer, Integer> result = rdd1.cartesian(rdd2);
        result.collect().forEach(System.out::println);
        sc.close();
    }

}
