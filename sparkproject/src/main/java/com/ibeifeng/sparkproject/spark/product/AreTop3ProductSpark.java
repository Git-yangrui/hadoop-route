package com.ibeifeng.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.domain.AreaTop3;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.service.AreTop3Serive;
import com.ibeifeng.sparkproject.service.TaskService;
import com.ibeifeng.sparkproject.spark.session.UserVisitSessionAnalyzerSpark;
import com.ibeifeng.sparkproject.spark.udf.ContactLongStringUDF;
import com.ibeifeng.sparkproject.spark.udf.GetJsonUDF;
import com.ibeifeng.sparkproject.spark.udf.GroupConcatDistinctUDAF;
import com.ibeifeng.sparkproject.util.Constants;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.Tuple2;

import java.util.*;

public class AreTop3ProductSpark {

    private static Logger LOG = LoggerFactory.getLogger(UserVisitSessionAnalyzerSpark.class);

    private static ApplicationContext context = new ClassPathXmlApplicationContext("classpath:application-core.xml");

    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setAppName(Constants.ARETOP3PRODUCTSPARK);
        SparkUtils.setMaster(conf);

        JavaSparkContext jsc=new JavaSparkContext(conf);
        SQLContext sqlContext =  SparkUtils.getSqlContext(jsc.sc());
        //注册UDF 函数
        sqlContext.udf().register("concat_long_string",new ContactLongStringUDF(),DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct",new GroupConcatDistinctUDAF());
        sqlContext.udf().register("get_json_object",new GetJsonUDF(),DataTypes.StringType);

        SparkUtils.mockData(jsc,sqlContext);

        Long taskIdFromArgs = ParamUtils.getTaskIdFromArgs(Constants.SPARK_LOCAL_TASKID_PRODUCT, args);
        TaskService bean = (TaskService)context.getBean("taskService");
        Task task = bean.findById(taskIdFromArgs);

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        //在某个城市发生的点击行为
        JavaPairRDD<Long,Row> clickActionByDateRdd = getClickActionByDate(sqlContext, taskParam);
//        SparkContext context = clickActionByDateRdd.context();
        JavaPairRDD<Long,Row> cityInfoRDD = getCityInfoRDD(sqlContext);

        generateClickProdcutBasicTmpTable(clickActionByDateRdd,cityInfoRDD,sqlContext);

        generateTempTableAreaProductClickCountTable(sqlContext);
        
        generateTempAreaFullProductClickCountTable(sqlContext);

        //生成包含完整的商品信息的临时表
        JavaRDD<Row> top3ProductRDD = getTop3ProductRDD(sqlContext);
        List<Row> collect = top3ProductRDD.collect();
        persistTop3ToDB(collect,taskIdFromArgs);
        jsc.close();
    }

    private static void persistTop3ToDB(List<Row> collect,long taskId) {

        List<AreaTop3> lists=new ArrayList<>();
        AreTop3Serive areTop3Serive=(AreTop3Serive)context.getBean("AreTop3Serive");
        collect.forEach(row -> {
            AreaTop3 areaTop3Product = new AreaTop3();
            areaTop3Product.setTaskid(taskId);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductId(Long.valueOf(row.getString(2)));
            areaTop3Product.setCliclCount(Long.valueOf(row.getString(3)));
            areaTop3Product.setCityNames(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));
            lists.add(areaTop3Product);

        });

        areTop3Serive.inserAll(lists);
    }

    private static JavaRDD<Row> getTop3ProductRDD(SQLContext sqlContext) {

        String sql="select " +
                "city_area," +
                "CASE" +
                   "  when city_area='华北' or city_area='华东' then 'A级'  "+
                   "  when city_area='华南' or city_area='华中' then 'B级'  "+
                   "  when city_area='西北' or city_area='西南' then 'C级'  "+
                   "  else 'D级'"+
                   " end city_area_level"+
                "product_id ," +
                "click_count," +
                "city_infos," +
                "product_name," +
                "product_status" +
                "from ( "+
                    "select " +
                    " city_area,product_id ,click_count,city_infos,product_name,product_status" +
                    " ROW_NUMBER() OVER(PARTITION by city_area order by click_count desc ) rank"+
                    "from tmp_area_fullprod_click_count where rank <=3" +
                ") t ";

        return sqlContext.sql(sql).javaRDD();
    }

    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {

        String sql="select " +
                "tapcc.city_area," +
                "tapcc.product_id," +
                "tapcc.click_count," +
                "tapcc.city_infos," +
                "pi.product_name," +
                "if(get_json_object(pi.extend_info,'product_status')=0,'ziying','disanfang') product_status " +
                "from tmp_area_product_click_count tapcc"  +
                " join product_info  pi" +
                "on pi.product_id = tapcc.product_id";
        Dataset<Row> df = sqlContext.sql(sql);
        df.registerTempTable("tmp_area_fullprod_click_count");

    }

    private static void generateTempTableAreaProductClickCountTable(SQLContext sqlContext) {

        String sql=
                "select city_area," +
                        "product_id ," +
                        "count(*) click_count"+
                       "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos" +
                "from tmp_clk_prod_basic group by city_area, product_id";
        Dataset<Row> df = sqlContext.sql(sql);

        df.registerTempTable("tmp_area_product_click_count");
    }

    private static void generateClickProdcutBasicTmpTable(JavaPairRDD<Long,Row> clickActionByDateRdd,JavaPairRDD<Long,Row> cityInfoRDD,SQLContext sqlContext){
        JavaPairRDD<Long, Tuple2<Row, Row>> joinResultsRDD = clickActionByDateRdd.join(cityInfoRDD);
        JavaRDD<Row> mapROWRDD = joinResultsRDD.map(v1 -> {
            Long cityid = v1._1;
            Row citiAction = v1._2._1;
            Row cityInfo = v1._2._2;
            long productid = citiAction.getLong(1);
            String cityName = cityInfo.getString(1);
            String cityArea = cityInfo.getString(2);

            return RowFactory.create(cityid, productid, cityName, cityArea);
        });

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("city_id", DataTypes.LongType, true),
                DataTypes.createStructField("product_id", DataTypes.LongType, true),
                DataTypes.createStructField("city_name", DataTypes.StringType, true),
                DataTypes.createStructField("city_area", DataTypes.LongType, true)));

        Dataset<Row> df = sqlContext.createDataFrame(mapROWRDD, schema);
        df.registerTempTable("tmp_clk_prod_basic");
    }

    private static JavaPairRDD<Long,Row> getClickActionByDate(SQLContext sqlContext, JSONObject taskParam) {
        String startDateparam = ParamUtils.getParam(taskParam, Constants.START_DATE);
        String endDateparam = ParamUtils.getParam(taskParam, Constants.END_DATE);
        String sql = "select city_id,click_product_id as product_id from user_visit_action where " +
                " click_product_id is not null and click_product_id !='NULL' and click_product_id !='null '" +
                " and action_time >='" + startDateparam + "'" +
                "and action_time <= '" + endDateparam + "'";
        Dataset<Row> dateset = sqlContext.sql(sql);
        dateset.groupBy("11");
        return dateset.javaRDD().mapToPair(row -> {
            return new Tuple2<Long,Row>(Long.valueOf(row.getString(0)),row);
        });

    }

    /**
     * sprak Sql从MySQL 查询数据
     * @param sqlContext
     * @return
     */
    private static JavaPairRDD<Long,Row> getCityInfoRDD(SQLContext sqlContext){

        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);

        Map<String,String> optionInfoForDb=new HashMap<>();
        optionInfoForDb.put("url",url);

        optionInfoForDb.put("dbtable","city_info");

        Dataset<Row> jdbcdataset = sqlContext.read().format("jdbc")
                .options(optionInfoForDb).load();

        return jdbcdataset.javaRDD().mapToPair(row -> {
            return new Tuple2<Long,Row>(row.getLong(0),row);
        });

    }
}
