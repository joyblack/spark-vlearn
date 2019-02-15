package com.spark.partition;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class PartitionTest2 {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(com.spark.transformation.AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<Integer,String> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2(1, "I"),
                new Tuple2(2, "love"),
                new Tuple2(3, "you"),
                new Tuple2(4, "too")
        ), 8);

        Optional<Partitioner> partitioner = rdd.partitioner();

        if(partitioner.isPresent()){
            System.out.println("partition info:" + partitioner.get());
        }else{
            System.out.println("Now no partitioner");
        }
        // 使用Hash分区器重新分区
        final JavaPairRDD<Integer, String> rdd2 = rdd.partitionBy(new HashPartitioner(3));

        Optional<Partitioner> partitioner1 = rdd2.partitioner();
        if(partitioner1.isPresent()){
            System.out.println("partition info:" + partitioner1.get());
        }else{
            System.out.println("Now no partitioner");
        }


        sc.close();


    }
}
