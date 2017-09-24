package com.ibeifeng.sparkproject.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.service.TaskService;
import com.ibeifeng.sparkproject.spark.session.UserVisitSessionAnalyzerSpark;
import com.ibeifeng.sparkproject.util.Constants;
import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.Int;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转换率模块 spark 作业
 */
public class PageOneStepConverRateSpark {

    private static Logger LOG = LoggerFactory.getLogger(UserVisitSessionAnalyzerSpark.class);

    private static ApplicationContext context = new ClassPathXmlApplicationContext("classpath:application-core.xml");

    public static void main(String[] args) {
        // 1.构造spark 上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_PAGE_APP);
        SparkUtils.setMaster(conf);
        JavaSparkContext jsc=new JavaSparkContext(conf);


        //细节见SparkUtils.getSqlContext
        SQLContext sqlContext=SparkUtils.getSqlContext(jsc.sc());

        SparkUtils.mockData(jsc,sqlContext);

        Long taskId = ParamUtils.getTaskIdFromArgs(Constants.SPARK_LOCAL_TASKID_PAGE,args);

        TaskService service = (TaskService) context.getBean("taskService");
        Task task = service.findById(taskId);
        if(task==null){
            LOG.error("can not  find this task id {} ",taskId);
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);

        JavaPairRDD<String, Row> sessionId2rowActionRDD = SparkUtils.getSessionId2rowActionRDD(actionRDD);
        sessionId2rowActionRDD = sessionId2rowActionRDD.cache();

        JavaPairRDD<String, Iterable<Row>> session2ActionRDD = sessionId2rowActionRDD.groupByKey();

        String targetFlowParam = ParamUtils.getParam(taskParam, Constants.PARAM_TRAGE_PAGE_FLOW);

        final Broadcast<String> broadcastTargetFlowParam = jsc.broadcast(targetFlowParam);

        JavaPairRDD<String, Integer> pageSpilitRdd = generateAndMatchPageSplit(session2ActionRDD, broadcastTargetFlowParam);

        Map<String, Long> pageidPVMap = pageSpilitRdd.countByKey();

        //3,2,5,8,6
        //3-2  2-5 5-8

        long pageidCount = getPageidCount(session2ActionRDD, targetFlowParam.split("\\,")[0]);

        String[] splitParam = broadcastTargetFlowParam.value().split(",");
        for (int i = 1; i < splitParam.length; i++) {
            String targetsplit = splitParam[i - 1] + "_" + splitParam[i];
            Long  targetsplitpv = pageidPVMap.get(targetsplit);
            double convertRate=0.0;


            if(i==1){
//                convertRate=
            }
        }

        pageidPVMap.forEach((pageIdSplit,count)->{
            pageIdSplit.split("\\_");
        });
    }

    private static long getPageidCount(JavaPairRDD<String, Iterable<Row>> session2ActionRDD, String s) {
        final  long startPageid = Long.valueOf(s);
        return session2ActionRDD.flatMap(stringIterableTuple2 -> {
            List<Long> list = new ArrayList<>();

            Iterable<Row> rows = stringIterableTuple2._2;
            for (Row row : rows) {
                long pageid = row.getLong(3);
                if (pageid == startPageid) {
                    list.add(pageid);
                }

            }
            return list.iterator();

        }).count();
    }

    private static JavaPairRDD<String, Integer>  generateAndMatchPageSplit(JavaPairRDD<String, Iterable<Row>> session2ActionRDD, Broadcast<String> broadcastTargetFlowParam) {
        return session2ActionRDD.flatMapToPair(iterableTuple -> {

            //每个sessionid 对应的访问数据
            String sessionid = iterableTuple._1;
            Iterable<Row> rows = iterableTuple._2;

            // 获取使用者指定的页面流
            // 使用者指定的页面流，1,2,3,4,5,6,7
            // 1->2的转化率是多少？2->3的转化率是多少？
            String[] splitParam = broadcastTargetFlowParam.value().split(",");

            List<Tuple2<String, Integer>> list = new ArrayList<>();

            List<Row> listrows = new ArrayList<>();
            rows.forEach(row -> {
                listrows.add(row);

            });
            //将 所有访问session按照action Time 排序
            Collections.sort(listrows, (o1, o2) -> {
                String actionTime1 = o1.getString(4);
                String actionTime2 = o2.getString(4);

                Date date1 = DateUtils.parseTime(actionTime1);
                Date date2 = DateUtils.parseTime(actionTime2);

                return (int) (date1.getTime() - date2.getTime());

            });
            Long lastPageid = null;
            for (Row row : listrows) {
                long pageid = row.getLong(3);
                if (lastPageid == null) {
                    lastPageid = pageid;
                    continue;
                }

                String pageiDSplit = lastPageid + "_" + pageid;
                //用户制定类似与 1,2,3,4,5
                //计算1-2  2-3  3-4 4-5 的数据量
                //一下算法可以这样去拼接处  然后可我们便利出的的进行比较
                for (int i = 1; i < splitParam.length; i++) {
                    String targetsplit = splitParam[i - 1] + "_" + splitParam[i];
                    if (targetsplit.equals(pageiDSplit)) {
                        list.add(new Tuple2<String, Integer>(pageiDSplit, 1));
                        break;
                    }

                }

                lastPageid = pageid;

            }
            return list.iterator();

        });
    }


}
