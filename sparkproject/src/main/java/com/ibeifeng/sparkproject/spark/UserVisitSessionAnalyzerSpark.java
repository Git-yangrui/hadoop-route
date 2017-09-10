package com.ibeifeng.sparkproject.spark;


import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.domain.*;
import com.ibeifeng.sparkproject.mockdata.MockData;
import com.ibeifeng.sparkproject.service.*;
import com.ibeifeng.sparkproject.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
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
        //改变集合类 为fastUtil类
        //广播大变量  sc.broadcast()
        //jvm 

        String[] s = new String[]{"1"};
        SparkConf conf = new SparkConf().setAppName(Constants.USERVISI_TSESSIONANALYZER_SPARK)
                .setMaster(SparkModelConstants.LOCAL).set("spark.storage.memoryFraction","0.5");//设置cache和对内存
        conf.set("spark.testing.memory", "471859200");
        conf.set("spark.serializer","org.apache.spark.serilizer.KryoSerializer");//序列化机制
        conf.registerKryoClasses(new Class[]{});//需要序列化的类
        TaskService service = (TaskService) context.getBean("taskService");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSqlContext(sc.sc());
        mockData(sc, sqlContext);
        Long taskId = ParamUtils.getTaskIdFromArgs(s);
        Task task = service.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        JavaRDD<Row> actionRDDByDateRange = getActionRDDByDateRange(sqlContext, taskParam);
        //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<Session aggregation count>>>>>>>>>>>>>>>>>>>>>>
        // sessionid,row  list
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId_rowActionRDD(actionRDDByDateRange);

        //到这里为止数据的格式是 《sessionid,(session,searchkeywords,clickcategoryids,age,professional ...............)》
        JavaPairRDD<String, String> sessionAggrInfoRDD = aggregateBySession(sqlContext, sessionId2ActionRDD);

        SessionAggrAccumulator sessionAggrAccumulator = new SessionAggrAccumulator();
        sc.sc().register(sessionAggrAccumulator);
        JavaPairRDD<String, String> filterRDD = filterSessionAndAggrStat(sessionAggrInfoRDD, taskParam, sessionAggrAccumulator);
        randomExtractSession(filterRDD, taskId, sessionId2ActionRDD, sessionAggrAccumulator);
//        //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<Session aggregation count>>>>>>>>>>>>>>>>>>>>>>
//
//
//        calculateAndPersistAggrStat(sessionAggrAccumulator.value(), task.getTaskId());

        //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<Session Top 10 >>>>>>>>>>>>>>>>>>>>>>

        JavaPairRDD<String, Row> commonSessionDetailRDD = getCommonSessionDetailRDD(filterRDD, sessionId2ActionRDD);
        List<Tuple2<CategorySortKey, String>> top10CateGoryList = getTop10CateGory(commonSessionDetailRDD, task.getTaskId());
        //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<Session Top 10 >>>>>>>>>>>>>>>>>>>>>>
        // 计算出各个范围的session占比，并写入MySQL
        getTop10Session(sc, task.getTaskId(), top10CateGoryList, commonSessionDetailRDD);

        sc.close();
    }

    private static void getTop10Session(JavaSparkContext sc, long taskId, List<Tuple2<CategorySortKey, String>> top10CateGoryList,
                                        JavaPairRDD<String, Row> commonSessionDetailRDD) {

        List<Tuple2<Long, Long>> top10Category = new ArrayList<>();
        top10CateGoryList.forEach(categorySortKeyStringTuple2 -> {
            long categoryid = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(
                    categorySortKeyStringTuple2._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10Category.add(new Tuple2<Long, Long>(categoryid, categoryid));
        });
        JavaPairRDD<Long, Long> categoryPairRDD = sc.parallelizePairs(top10Category);
        JavaPairRDD<String, Iterable<Row>> sessionWithREcordRDD = commonSessionDetailRDD.groupByKey();

        JavaPairRDD<Long, String> categoryCOunt = sessionWithREcordRDD.flatMapToPair(stringIterableTuple2 -> {
            String sessionid = stringIterableTuple2._1;

            Iterable<Row> rows = stringIterableTuple2._2;
            Map<Long, Long> cateGoryClickMap = new HashMap<>();
            rows.forEach(row -> {
                if (row.get(6) != null) {
                    Long categoryid = row.getLong(6);
                    Long count = cateGoryClickMap.get(categoryid);

                    if (count == null) {
                        count = 0L;
                    }
                    count++;
                    cateGoryClickMap.put(categoryid, count);
                }
            });

            List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();

            cateGoryClickMap.forEach((categoryid, categoryidCount) -> {
                String value = sessionid + "," + categoryidCount;
                list.add(new Tuple2<Long, String>(categoryid, value));

            });

            return list.iterator();

        });

       //  top 10 categoryid and with session with count
        JavaPairRDD<Long, Tuple2<Long, String>> top10CategorywithSessionCount = categoryPairRDD.join(categoryCOunt);
        JavaPairRDD<Long, String> categoryIdwithSessionCountRDD = top10CategorywithSessionCount.mapToPair(longTuple2Tuple2 -> {

            Long categoryid = longTuple2Tuple2._1;
            String sessionwithCount = longTuple2Tuple2._2._2;
            return new Tuple2<Long, String>(categoryid, sessionwithCount);
        });
        JavaPairRDD<Long, Iterable<String>> categoryWithAllSessionCount = categoryIdwithSessionCountRDD.groupByKey();


        JavaPairRDD<String, String> stringStringJavaPairRDD = categoryWithAllSessionCount.flatMapToPair(longIterableTuple2 -> {

            Long categoryid = longIterableTuple2._1;

            String[] topSessoion = new String[10];
            Iterator<String> iterator = longIterableTuple2._2.iterator();
            iterator.forEachRemaining(sessionWithCount -> {

                long count = Long.valueOf(sessionWithCount.split("\\,")[1]);

                for (int i = 0; i < topSessoion.length; i++) {
                    if (topSessoion[i] == null) {
                        topSessoion[i] = sessionWithCount;
                        break;
                    } else {

                        long _count = Long.valueOf(topSessoion[i].split("\\,")[1]);
                        if (count > _count) {
                            for (int j = 9; j > i; j--) {
                                topSessoion[j] = topSessoion[j - 1];

                            }
                            topSessoion[i] = sessionWithCount;
                            break;
                        }

                    }

                }
            });

            List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
            Arrays.asList(topSessoion).forEach(sessionCount -> {
                String sessionid = sessionCount.split(",")[0];
                long count = Long.valueOf(sessionCount.split(",")[1]);
                // 放入list
                list.add(new Tuple2<String, String>(sessionid, sessionid));

            });

            return list.iterator();
        });

    }


    private static JavaPairRDD<String, Row> getCommonSessionDetailRDD(JavaPairRDD<String, String> filterRDD, JavaPairRDD<String, Row> sessionId2ActionRDD) {
        JavaPairRDD<String, Tuple2<String, Row>> filterSessionWithSessionRow = filterRDD.join(sessionId2ActionRDD);
        JavaPairRDD<String, Row> detailSession = filterSessionWithSessionRow.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> stringTuple2Tuple2) throws Exception {
                return new Tuple2<String, Row>(stringTuple2Tuple2._1, stringTuple2Tuple2._2._2);
            }
        });


        return detailSession;
    }

    //获得(session_id,ROW)  .........

    private static JavaPairRDD<String, Row> getSessionId_rowActionRDD(JavaRDD<Row> actionRDDByDateRange) {

        return actionRDDByDateRange.mapToPair(new PairFunction<Row, String, Row>() {

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });
    }

    private static JavaPairRDD<String, String> aggregateBySession(SQLContext sc, JavaPairRDD<String, Row> sessionId2ActionRDD) {


        //groupByKey  获得这种类型的   （sessionid,(ROW1,ROW2)） 聚合同样的sessionid
        JavaPairRDD<String, Iterable<Row>> seessionGroupByRDD = sessionId2ActionRDD.groupByKey();

        //mapToPair操作 进行便利操作
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

                    String string = row.getString(4);
                    //计算session开始和结束的时间
                    Date actiontime = DateUtils.parseTime(string);
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
                        + Constants.FIELD_STARTTIME + "=" + DateUtils.formatTime(starTime);
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
                String sessionid = com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(partaggrInfo, "\\|",
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


    private static List<Tuple2<CategorySortKey, String>> getTop10CateGory(JavaPairRDD<String, Row> commonSessionDetailRDD, final long taskid) {

        JavaPairRDD<Long, Long> resulte = commonSessionDetailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

            @Override
            public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> stringRowTuple2) throws Exception {

                Row row = stringRowTuple2._2;

                List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                Object o = row.get(6);
                if (o != null) {
                    long clickCategoryId = row.getLong(6);
                    list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                }

                String orderCategoryIds = row.getString(8);
                if (orderCategoryIds != null) {
                    String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                    for (String orderCategoryId : orderCategoryIdsSplited) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
                                Long.valueOf(orderCategoryId)));
                    }
                }

                String payCategoryIds = row.getString(10);
                if (payCategoryIds != null) {
                    String[] payCategoryIdsSplited = payCategoryIds.split(",");
                    for (String payCategoryId : payCategoryIdsSplited) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
                                Long.valueOf(payCategoryId)));
                    }
                }


                return list.iterator();
            }
        });
        resulte = resulte.distinct();
        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(commonSessionDetailRDD);
        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(commonSessionDetailRDD);
        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(commonSessionDetailRDD);

        JavaPairRDD<Long, String> longStringJavaPairRDD = joinCategoryAndData(resulte, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);

        //二次排序  自定义二次排序

        JavaPairRDD<CategorySortKey, String> categorySortKeyStringJavaPairRDD = longStringJavaPairRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {

            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> longStringTuple2) throws Exception {
                String countinfo = longStringTuple2._2;
                long clickCount = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.
                        getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_CLICK_COUNT));
                long orderCount = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.
                        getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_ORDER_COUNT));
                long payCount = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.
                        getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_PAY_COUNT));

                CategorySortKey categorySortKey = new CategorySortKey(clickCount, orderCount, payCount);
                return new Tuple2<CategorySortKey, String>(categorySortKey, countinfo);
            }
        });

        JavaPairRDD<CategorySortKey, String> categorySortKeyStringJavaPairRDD1 = categorySortKeyStringJavaPairRDD.sortByKey(false);

        List<Tuple2<CategorySortKey, String>> top10 = categorySortKeyStringJavaPairRDD1.take(10);

        ITop10CategoryService iTop10CategoryService = (ITop10CategoryService) context.getBean("iTop10CategoryService");


        for (Tuple2<CategorySortKey, String> eachTuple : top10) {
            String countinfo = eachTuple._2;

            long caterGoryId = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.
                    getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.
                    getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.
                    getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(com.ibeifeng.sparkproject.util.StringUtils.
                    getFieldFromConcatString(countinfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Catefory top10Catefory = new Top10Catefory();

            top10Catefory.setTaskid(taskid);

            top10Catefory.setCategoryid(caterGoryId);

            top10Catefory.setClickCount(clickCount);

            top10Catefory.setOrderCount(orderCount);

            top10Catefory.setPayCount(payCount);

            iTop10CategoryService.insert(top10Catefory);

        }
        return top10;
    }


    private static void randomExtractSession(JavaPairRDD<String, String> filterRDD, final long taskid, JavaPairRDD<String, Row> sessionId2ActionRDD

            , SessionAggrAccumulator sessionAggrAccumulator) {

        // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD111 = filterRDD.mapToPair(

                new PairFunction<Tuple2<String, String>, String, String>() {

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

        // 得到每天每小时的session数量<1,34><2,35><3,45>  ....................
        Map<String, Long> countMap = time2sessionidRDD111.countByKey();

//        calculateAndPersistAggrStat(sessionAggrAccumulator.value(), taskid);
        // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引

        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap =
                new HashMap<String, Map<String, Long>>();

        for (Map.Entry<String, Long> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
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

        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每个小时
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                // 就可以计算出，当前小时需要抽取的session数量
                int hourExtractNumber = (int) (((double) count / (double) sessionCount)
                        * extractNumberPerDay);
                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                // 先获取当前小时的存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }
        //  dateHour   aggrInfo,aggrInfo,aggrInfo
        JavaPairRDD<String, Iterable<String>> timeSessionSRDD = time2sessionidRDD111.groupByKey();
        JavaPairRDD<String, String> extractJavaPairRDD = timeSessionSRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

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

        JavaPairRDD<String, Tuple2<String, Row>> sessionIdJoinrow = extractJavaPairRDD.join(sessionId2ActionRDD);

        sessionIdJoinrow.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> stringTuple2Tuple2) throws Exception {
                String sessionid = stringTuple2Tuple2._1;
                Row row = stringTuple2Tuple2._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyWord(row.getString(5));
                long clickCatesgoryId = 0;
                try {
                    clickCatesgoryId = row.getLong(6);
                    sessionDetail.setClickCategoryId(clickCatesgoryId);
                } catch (Exception e) {
                    sessionDetail.setClickCategoryId(clickCatesgoryId);
                }
                long clickProductId = 0;
                try {
                    clickProductId = row.getLong(7);
                    sessionDetail.setClickProductId(clickProductId);
                } catch (Exception e) {
                    sessionDetail.setClickProductId(clickProductId);
                }
                sessionDetail.setOrdercategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                SessionDetailService sessionDetailService = (SessionDetailService) context.getBean("sessionDetailService");
                sessionDetailService.insert(sessionDetail);
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

        JavaPairRDD<String, String> filterRDd = rdd.filter(new Function<Tuple2<String, String>, Boolean>() {
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

        return filterRDd;
    }


    /**
     * 获取各品类的下单次数RDD
     *
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(

                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(8) != null ? true : false;
                    }

                });

        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Long, Long>> call(
                            Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String orderCategoryIds = row.getString(8);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                        }

                        return list.iterator();
                    }

                });

        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        return orderCategoryId2CountRDD;
    }

    /**
     * 获取各个品类的支付次数RDD
     *
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(

                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(10) != null ? true : false;
                    }

                });

        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Long, Long>> call(
                            Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String payCategoryIds = row.getString(10);
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
                        }

                        return list.iterator();
                    }

                });

        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        return payCategoryId2CountRDD;
    }


    /**
     * 获取各品类点击次数RDD
     *
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(

                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.get(6) != null ? true : false;

                    }
                });

        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(

                new PairFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple)
                            throws Exception {
                        long clickCategoryId = tuple._2.getLong(6);
                        return new Tuple2<Long, Long>(clickCategoryId, 1L);
                    }

                });

        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        return clickCategoryId2CountRDD;
    }

    /**
     * 连接品类RDD与数据RDD
     *
     * @param categoryidRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long, String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryidRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        // 解释一下，如果用leftOuterJoin，就可能出现，右边那个RDD中，join过来时，没有值
        // 所以Tuple中的第二个值用Optional<Long>类型，就代表，可能有值，可能没有值
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD =
                categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        long clickCount = 0L;

                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" +
                                Constants.FIELD_CLICK_COUNT + "=" + clickCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long orderCount = 0L;

                        if (optional.isPresent()) {
                            orderCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long payCount = 0L;

                        if (optional.isPresent()) {
                            payCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });

        return tmpMapRDD;
    }

}



