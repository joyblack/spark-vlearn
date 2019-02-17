package com.spark.sql;

import com.spark.input.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class RddWithDatasetTest2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local").set("spark.testing.memory", "2140000000");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().appName("sql").config(conf).getOrCreate();

        // 1.创建一个多行结构的RDD
        JavaRDD<Row> rdd = sc.parallelize(Arrays.asList(RowFactory.create(1, "zhaoyi"),
                RowFactory.create(2, "hezhen"),
                RowFactory.create(3, "akuya"),
                RowFactory.create(4, "huihui"),
                RowFactory.create(4, "crasetina")
        ));

        // 2.创建用StructType类型的类(即schema)，用来表示的行结构信息
        List<StructField> fields = Arrays.asList(DataTypes.createStructField("id", DataTypes.IntegerType,true),
                DataTypes.createStructField("name", DataTypes.StringType,true)
                );
        StructType schema = DataTypes.createStructType(fields);

        // 3.通过SparkSession提供的createDataFrame方法来应用Schema
        Dataset<Row> userDF = spark.createDataFrame(rdd, schema);

        userDF.show();




    }
}
