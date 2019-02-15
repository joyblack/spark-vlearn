package com.spark.action;

import com.spark.transformation.AggregateTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class TakeSampleTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(90, 20, 10, 50, 60, 70, 80, 30, 100, 40));
        System.out.println("1.(withReplacement = false)The rdd takeSample result is :" + rdd.takeSample(false, 20));
        System.out.println("2.(withReplacement = true) The rdd takeSample result is :" + rdd.takeSample(true, 20));
        sc.close();
    }
}
