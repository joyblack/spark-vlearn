package com.spark.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class HadoopAPITest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(com.spark.transformation.AggregateTest.class);
        SparkConf sparkConf = new SparkConf().setAppName("test").set("spark.testing.memory", "2147480000").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>(1, "zhaoyi"), new Tuple2<>(2, "hongqun")));
        // 写入本地文件系统
        pairRDD.saveAsNewAPIHadoopFile("out_h",
                IntWritable.class,
                Text.class,
                TextOutputFormat.class
        );



        // 读出
        JavaPairRDD<LongWritable, Text> readRDD = sc.newAPIHadoopFile("out_h", TextInputFormat.class,
                LongWritable.class,
                Text.class,
                new Configuration()
        );

        readRDD.foreach(s -> System.out.println(s._2));

        sc.close();


    }
}
