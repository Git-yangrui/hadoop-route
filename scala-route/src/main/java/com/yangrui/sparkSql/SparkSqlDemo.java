package com.yangrui.sparkSql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Collections;

public class SparkSqlDemo {

    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf().setAppName("SparkSqlDemo").setMaster("local");
//        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        SQLContext sqlContext=new SQLContext(javaSparkContext);
//        JavaRDD<Person> people = javaSparkContext.textFile("C:\\Users\\admin\\Desktop\\people.txt").map(
//                new Function<String, Person>() {
//                    public Person call(String line) throws Exception {
//                        String[] parts = line.split(",");
//                        Person person = new Person();
//                        person.setName(parts[0]);
//                        person.setAge(Integer.parseInt(parts[1].trim()));
//                        return person;
//                    }
//                });
//
//        DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
//        schemaPeople.registerTempTable("people");
//        DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");
//        List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
//                public String call(Row row) {
//                    return "Name: " + row.getString(0);
//                }
//        }).collect();
//
//        javaSparkContext.close();


        /**
         * spark 2.1.0 change API
         *
         */

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("C:\\Users\\yangrui\\Desktop\\people.txt")
                .javaRDD()
                .map(new Function<String, Person>() {
                    @Override
                    public Person call(String line) throws Exception {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));
                        return person;
                    }
                });

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);

        peopleDF.createOrReplaceTempView("people");


        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(0);
            }
        }, stringEncoder);
        teenagerNamesByIndexDF.show();

    }
}
