package com.spark.action;

import com.spark.transformation.AggregateTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class NumberOperateTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(90D, 20D, 10D, 50D, 60D, 70D, 80D, 30D, 100D, 40D));
        System.out.println("*** count: " + rdd.count());
        System.out.println("*** mean: " + rdd.mean());
        System.out.println("*** sum: " + rdd.sum());
        System.out.println("*** min: " + rdd.min());
        System.out.println("*** max: " + rdd.max());
        System.out.println("*** variance: " + rdd.variance());
        System.out.println("*** sampleVariance: " + rdd.sampleVariance());
        System.out.println("*** stdev: " + rdd.stdev());
        System.out.println("*** sampleStdev: " + rdd.sampleStdev());


        sc.close();
    }
}
