package com.yangrui.sparkSql;

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class SparkSqlDemo {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkSqlDemo").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext=new SQLContext(javaSparkContext);
        JavaRDD<Person> people = javaSparkContext.textFile("C:\\Users\\admin\\Desktop\\people.txt").map(
                new Function<String, Person>() {
                    public Person call(String line) throws Exception {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));
                        return person;
                    }
                });
        DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
        schemaPeople.registerTempTable("people");
        DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");
        List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
                public String call(Row row) {
                    return "Name: " + row.getString(0);
                }
        }).collect();

        javaSparkContext.close();

    }
}
