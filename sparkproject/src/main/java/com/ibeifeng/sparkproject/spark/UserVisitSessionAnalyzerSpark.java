package com.ibeifeng.sparkproject.spark;


import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.mockdata.MockData;
import com.ibeifeng.sparkproject.service.TaskService;
import com.ibeifeng.sparkproject.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;


public class UserVisitSessionAnalyzerSpark {

    private static Logger LOG = LoggerFactory.getLogger(UserVisitSessionAnalyzerSpark.class);

    private static ApplicationContext context = new ClassPathXmlApplicationContext("classpath:application-core.xml");

    public static void main(String[] args) {
        String[] s = new String[]{"1"};
        SparkConf conf = new SparkConf().setAppName(Constants.USERVISI_TSESSIONANALYZER_SPARK)
                .setMaster(SparkModelConstants.LOCAL);
        TaskService service = (TaskService) context.getBean("taskService");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSqlContext(sc.sc());
        mockData(sc, sqlContext);
        Long taskId = ParamUtils.getTaskIdFromArgs(s);
        Task task = service.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionRDDByDateRange = getActionRDDByDateRange(sqlContext, taskParam);

        //到这里为止数据的格式是 《sessionid,(session,searchkeywords,clickcategoryids,age,professional ...............)》
        JavaPairRDD<String, String> sessionAggrInfoRDD = aggregateBySession(sqlContext, actionRDDByDateRange);


        SessionAggrAccumulator sessionAggrAccumulator = new SessionAggrAccumulator();
        sc.sc().register(sessionAggrAccumulator);
        JavaPairRDD<String, String> filterRDD = filterSessionAndAggrStat(sessionAggrInfoRDD, taskParam, sessionAggrAccumulator);
        sc.close();
    }

    /**
     * 获取Sqlcontext
     *
     * @param sc
     * @return
     */
    private static SQLContext getSqlContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
//            SparkSession.Builder builder = SparkSession.builder().enableHiveSupport().sparkContext(sc);
            return new HiveContext(sc);
        }
    }

    private static void mockData(JavaSparkContext jsc, SQLContext sqlContext) {
        Boolean aBoolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (aBoolean) {
            MockData.mock(jsc, sqlContext);
        }
    }

    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDateparam = ParamUtils.getParam(taskParam, Constants.START_DATE);
        String endDateparam = ParamUtils.getParam(taskParam, Constants.END_DATE);
        String sql = "select * from user_visit_action where date >='" + startDateparam + "'" +
                "and date <= '" + endDateparam + "'";
        Dataset<Row> sql1 = sqlContext.sql(sql);
        return sql1.javaRDD();
    }

    private static JavaPairRDD<String, String> aggregateBySession(SQLContext sc, JavaRDD<Row> javaRdd) {
        JavaPairRDD<String, Row> sessionRowPairRDD = javaRdd.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });

        JavaPairRDD<String, Iterable<Row>> seessionGroupByRDD = sessionRowPairRDD.groupByKey();

        JavaPairRDD<Long, String> userAggrInfoRDD = seessionGroupByRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String sessionid = stringIterableTuple2._1;
                Iterator<Row> iterator = stringIterableTuple2._2.iterator();
                StringBuffer searchKeyWordBuffer = new StringBuffer("");
                StringBuffer clickCategoryBuffer = new StringBuffer("");
                Long userid = null;
                Date starTime = null;
                Date endTime = null;
                int stepLength = 0;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (userid == null) {
                        userid = row.getLong(1);
                    }
                    String serachKeyWord = row.getString(5);
                    Long clickCategoryID = null;
                    try {
                        clickCategoryID = row.getLong(6);
                    } catch (Exception e) {
                        clickCategoryID = null;
                    }

                    if (StringUtils.isNotEmpty(serachKeyWord)) {
                        if (!searchKeyWordBuffer.toString().contains(serachKeyWord)) {
                            searchKeyWordBuffer.append(serachKeyWord + ",");
                        }
                    }
                    if (clickCategoryID != null) {
                        if (!clickCategoryBuffer.toString().contains(String.valueOf(clickCategoryID))) {
                            clickCategoryBuffer.append(clickCategoryID + ",");
                        }
                    }
                    //计算session开始和结束的时间

                    Date actiontime = DateUtils.parseTime(row.getString(4));
                    if (starTime == null) {
                        starTime = actiontime;
                    }

                    if (endTime == null) {
                        endTime = actiontime;
                    }

                    if (starTime.after(actiontime)) {
                        starTime = actiontime;
                    }

                    if (actiontime.after(endTime)) {
                        endTime = actiontime;
                    }

                    //计算 session的访问步长
                    stepLength++;
                }
                String searchKeywords = com.ibeifeng.sparkproject.util.StringUtils.trimComma(searchKeyWordBuffer.toString());
                String clickCategoryIDs = com.ibeifeng.sparkproject.util.StringUtils.trimComma(clickCategoryBuffer.toString());
                //计算session访问的时长
                long visitLength = (endTime.getTime() - starTime.getTime()) / 1000;
                /**
                 * 聚合之后的数据怎么拼接
                 * key-value
                 */
                String partAggrInfo = Constants.FIELD_SESSIONID + "=" + sessionid + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CATEGORY_IDS + "=" + clickCategoryIDs + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength;
                return new Tuple2<Long, String>(userid, partAggrInfo);
            }
        });
        //查询所以的用户信息
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRdd = sc.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRdd.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {

                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        //join 操作
        JavaPairRDD<Long, Tuple2<String, Row>> joinResult = userAggrInfoRDD.join(userid2InfoRDD);
        JavaPairRDD<String, String> sessionAggrInfoRDD = joinResult.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> longTuple2Tuple2) throws Exception {
                String partaggrInfo = longTuple2Tuple2._2._1;
                String sessionid = com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(partaggrInfo, "|",
                        Constants.FIELD_SESSIONID);
                Row rowInfo = longTuple2Tuple2._2._2;

                int age = rowInfo.getInt(3);
                String professionl = rowInfo.getString(4);
                String city = rowInfo.getString(4);
                String sex = rowInfo.getString(4);
                String partaggrInfos = partaggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSINAL + "=" + professionl + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<String, String>(sessionid, partaggrInfos);
            }
        });

        return sessionAggrInfoRDD;
    }

    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD<String, String> rdd,
                                                                        final JSONObject taskParam,
                                                                        final SessionAggrAccumulator sessionAggrAccumulator) {

        String startAge = ParamUtils.getParam(taskParam, Constants.START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALs);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORYIDS);

        String _parameter = (startAge != null ? Constants.START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALs + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_PROFESSIONALs + "=" + categoryIds : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        JavaPairRDD<String, String> filterRDD = rdd.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {

                String aggrInfo = stringStringTuple2._2;
                // 接着，依次按照筛选条件进行过滤
                // 按照年龄范围进行过滤（startAge、endAge）
                if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                        parameter, Constants.START_AGE, Constants.END_AGE)) {
                    return false;
                }

                // 按照职业范围进行过滤（professionals）
                // 互联网,IT,软件
                // 互联网
                if (!ValidUtils.in(aggrInfo, Constants.PARAM_PROFESSIONALs,
                        parameter, Constants.PARAM_PROFESSIONALs)) {
                    return false;
                }

                // 按照城市范围进行过滤（cities）
                // 北京,上海,广州,深圳
                // 成都
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                        parameter, Constants.PARAM_CITIES)) {
                    return false;
                }

                // 按照性别进行过滤
                // 男/女
                // 男，女
                if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                        parameter, Constants.PARAM_SEX)) {
                    return false;
                }

                // 按照搜索词进行过滤
                // 我们的session可能搜索了 火锅,蛋糕,烧烤
                // 我们的筛选条件可能是 火锅,串串香,iphone手机
                // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                // 任何一个搜索词相当，即通过
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                        parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }

                // 按照点击品类id进行过滤
                if (!ValidUtils.in(aggrInfo, Constants.FIELD_CATEGORY_IDS,
                        parameter, Constants.PARAM_CATEGORYIDS)) {
                    return false;
                }

                sessionAggrAccumulator.add(Constants.SESSION_COUNT);
                String fieldFromConcatString = com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH);
                Long visitLength = null;
                if (StringUtils.isNotEmpty(fieldFromConcatString)) {
                    visitLength = Long.valueOf(fieldFromConcatString);
                }
                String stepLenthString = com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH);
                Long stepLength = null;
                if (StringUtils.isNotEmpty(stepLenthString)) {
                    stepLength = Long.valueOf(stepLenthString);
                }
                calculateStepLength(stepLength);
                calculateVisitLength(visitLength);
                return true;
            }

            /**
             * 计算访问时长范围
             * @param visitLength
             */
            private void calculateVisitLength(long visitLength) {
                if (visitLength >= 1 && visitLength <= 3) {
                    sessionAggrAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitLength >= 4 && visitLength <= 6) {
                    sessionAggrAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitLength >= 7 && visitLength <= 9) {
                    sessionAggrAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitLength >= 10 && visitLength <= 30) {
                    sessionAggrAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitLength > 30 && visitLength <= 60) {
                    sessionAggrAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitLength > 60 && visitLength <= 180) {
                    sessionAggrAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitLength > 180 && visitLength <= 600) {
                    sessionAggrAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitLength > 600 && visitLength <= 1800) {
                    sessionAggrAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitLength > 1800) {
                    sessionAggrAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            private void calculateStepLength(Long stepLength) {
                if (stepLength >= 1 && stepLength <= 3) {
                    sessionAggrAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if (stepLength >= 4 && stepLength <= 6) {
                    sessionAggrAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if (stepLength >= 7 && stepLength <= 9) {
                    sessionAggrAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if (stepLength >= 10 && stepLength <= 30) {
                    sessionAggrAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if (stepLength > 30 && stepLength <= 60) {
                    sessionAggrAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if (stepLength > 60) {
                    sessionAggrAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });


        return filterRDD;
    }
}
