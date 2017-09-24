package com.ibeifeng.sparkproject.util;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.mockdata.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

public class SparkUtils {

  public static void setMaster(SparkConf conf){
      Boolean aBoolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
      if (aBoolean) {
          conf.setMaster("local");
          conf.set("spark.testing.memory", "471859200");
      }
  }

    public static void mockData(JavaSparkContext jsc, SQLContext sqlContext) {
        Boolean aBoolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (aBoolean) {
            MockData.mock(jsc, sqlContext);
        }
    }

    public static SQLContext getSqlContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {

             /*
               脚本要将hive的配置文件放到脚本中  hive-site.xml 制定hive 的配置信息
             */
            SparkSession sparksession = SparkSession.builder().config("","").sparkContext(sc)
                    .enableHiveSupport().getOrCreate();
            sparksession.sparkContext()


            return  sparksession.sqlContext();
        }
    }

    public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDateparam = ParamUtils.getParam(taskParam, Constants.START_DATE);
        String endDateparam = ParamUtils.getParam(taskParam, Constants.END_DATE);
        String sql = "select * from user_visit_action where date >='" + startDateparam + "'" +
                "and date <= '" + endDateparam + "'";
        Dataset<Row> sql1 = sqlContext.sql(sql);
        return sql1.javaRDD();
    }

    public static JavaPairRDD<String, Row> getSessionId2rowActionRDD(JavaRDD<Row> actionRDDByDateRange) {

        return actionRDDByDateRange.mapToPair(row ->{
                return new Tuple2<String, Row>(row.getString(2), row);
        });
    }



}
