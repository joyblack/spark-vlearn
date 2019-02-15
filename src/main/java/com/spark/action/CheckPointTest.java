package com.spark.action;

import com.spark.transformation.AggregateTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class CheckPointTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        sc.setCheckpointDir("checkDir");
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a","b","c"));
        JavaRDD<String> noCheckRDD = rdd.map(s -> s + "_" + System.currentTimeMillis());
        // 触发
        noCheckRDD.collect();
        JavaRDD<String> checkRDD = rdd.map(s -> s + "_" + System.currentTimeMillis());
        checkRDD.checkpoint();
        checkRDD.collect();
        System.out.println("*** no checkRDD print ***");
        System.out.println("noCheckRDD 1: " + noCheckRDD.collect());
        System.out.println("noCheckRDD 2: " + noCheckRDD.collect());
        System.out.println("noCheckRDD 3: " + noCheckRDD.collect());

        System.out.println("*** checkRDD print ***");

        System.out.println("checkRDD 1: " + checkRDD.collect());
        System.out.println("checkRDD 2: " + checkRDD.collect());
        System.out.println("checkRDD 3: " +checkRDD.collect());

        sc.close();
    }
}
