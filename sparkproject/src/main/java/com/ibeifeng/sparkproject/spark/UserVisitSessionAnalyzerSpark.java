package com.ibeifeng.sparkproject.spark;


import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.domain.SessionAggrStat;
import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.mockdata.MockData;
import com.ibeifeng.sparkproject.service.ISessionAggrStatService;
import com.ibeifeng.sparkproject.service.ISessionRandomExtractSevice;
import com.ibeifeng.sparkproject.service.TaskService;
import com.ibeifeng.sparkproject.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.Tuple2;

import java.util.*;


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
        randomExtractSession(filterRDD,taskId);
        LOG.info("***********************************************************************" + filterRDD.count());
        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrAccumulator.value(), task.getTaskId());

        sc.close();
    }

    private static void randomExtractSession(JavaPairRDD<String, String> filterRDD,final long taskid) {

        // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD = filterRDD.mapToPair(

                new PairFunction<Tuple2<String,String>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<String, String> tuple) throws Exception {
                        String aggrInfo = tuple._2;

                        String startTime = com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STARTTIME);
                        String dateHour = DateUtils.getDateHour(startTime);

                        return new Tuple2<String, String>(dateHour, aggrInfo);
                    }

                });

        /**
         * 思考一下：这里我们不要着急写大量的代码，做项目的时候，一定要用脑子多思考
         *
         * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
         * 首先抽取出的session的聚合数据，写入session_random_extract表
         * 所以第一个RDD的value，应该是session聚合数据
         *
         */

        // 得到每天每小时的session数量
        Map<String, Long> countMap = time2sessionidRDD.countByKey();

        // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引

        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap =
                new HashMap<String, Map<String, Long>>();

        for(Map.Entry<String, Long> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        // 开始实现我们的按时间比例随机抽取算法

        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        // <date,<hour,(3,5,20,102)>>
       final Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                new HashMap<String, Map<String, List<Integer>>>();

        Random random = new Random();

        for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的session总数
            long sessionCount = 0L;
            for(long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if(hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每个小时
            for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                // 就可以计算出，当前小时需要抽取的session数量
                int hourExtractNumber = (int)(((double)count / (double)sessionCount)
                        * extractNumberPerDay);
                if(hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                // 先获取当前小时的存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if(extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for(int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while(extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }
        //  dateHour   aggrInfo,aggrInfo,aggrInfo
        JavaPairRDD<String, Iterable<String>> timeSessionSRDD = time2sessionidRDD.groupByKey();
        JavaRDD<Tuple2<String, String>> tuple2JavaRDD = timeSessionSRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<String>>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                List<Tuple2<String, String>> extractSessionids = new ArrayList<>();
                String dateHour = stringIterableTuple2._1;
                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];
                Iterable<String> iterable = stringIterableTuple2._2;

                Iterator<String> iterator = iterable.iterator();
                int index = 0;
                List<Integer> hourCOunlist = dateHourExtractMap.get(date).get(hour);
                ISessionRandomExtractSevice bean = (ISessionRandomExtractSevice) context.getBean("iSessionRandomExtractSevice");

                while (iterator.hasNext()) {
                    String sessionAggrInfo = iterator.next();
                    if (hourCOunlist.contains(index)) {
                        String sessionid = com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_SESSIONID);

                        // 将数据写入MySQL
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        sessionRandomExtract.setTaskid(taskid);
                        sessionRandomExtract.setSessionid(sessionid);
                        sessionRandomExtract.setStartTime(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_STARTTIME));
                        sessionRandomExtract.setSearchKeywords(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                        sessionRandomExtract.setClickCategoryIds(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                                sessionAggrInfo, "\\|", Constants.FIELD_CATEGORY_IDS));

                        bean.insert(sessionRandomExtract);
                        extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                    }

                    index++;
                }

                return extractSessionids.iterator();
            }
        });
    }

    private static void calculateAndPersistAggrStat(String value, long taskId) {
// 从Accumulator统计串中获取值
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        ISessionAggrStatService iSessionAggrStatService = (ISessionAggrStatService) context.getBean("iSessionAggrStatService");
        iSessionAggrStatService.insert(sessionAggrStat);

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
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_STARTTIME + "=" + DateUtils.formatDate(starTime);
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
