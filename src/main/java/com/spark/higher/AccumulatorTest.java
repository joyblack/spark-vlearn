package com.spark.higher;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class AccumulatorTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(com.spark.transformation.AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        LongAccumulator myAccumulator = sc.sc().longAccumulator("than100CountAccm");

        sc.parallelize(Arrays.asList(1, 2, 3, 200, 300)).foreach(num -> {
            if(num > 100){
                myAccumulator.add(1);
            }
        });
        System.out.println(myAccumulator.value());
    }
}
