package com.spark.higher;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class MyAccumulatorTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(com.spark.transformation.AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        MyAccumulator myAccumulator = new MyAccumulator();
        // 注册到sc中
        sc.sc().register(myAccumulator,"myAccumulator");



        sc.parallelize(Arrays.asList("red", "blue", "photo", "father", "mother")).foreach(noun -> {
            if(isColor(noun)){
                myAccumulator.add(noun);
            }
        });
        System.out.println(myAccumulator.value());
    }

    public static boolean isColor(String noun){
        List<String> colors = Arrays.asList("red", "blue");
        if(colors.contains(noun)){
            return true;
        }
        return false;
    }
}
