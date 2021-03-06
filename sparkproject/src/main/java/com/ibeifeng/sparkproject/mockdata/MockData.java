package com.ibeifeng.sparkproject.mockdata;

import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 模拟数据程序
 *
 * @author Administrator
 */
public class MockData {
    private static Logger log = LoggerFactory.getLogger(MockData.class);

    /**
     * 弄你数据
     *
     * @param sc
     * @param sqlContext
     */
    public static void mock(JavaSparkContext sc,
                            SQLContext sqlContext) {
        List<Row> rows = new ArrayList<Row>();

        String[] searchKeywords = new String[]{"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        String date = DateUtils.getTodayDate();
        String[] actions = new String[]{"search", "click", "order", "pay"};
        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            long userid = random.nextInt(100);

            for (int j = 0; j < 10; j++) {
                String sessionid = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + random.nextInt(23);

                for (int k = 0; k < random.nextInt(100); k++) {
                    long pageid = random.nextInt(10);
                    String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickCategoryId = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    if ("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(10)];
                    } else if ("click".equals(action)) {
                        clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                    } else if ("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if ("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }

                    Row row = RowFactory.create(date, userid, sessionid,
                            pageid, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds, random.nextInt(10));
                    rows.add(row);
                }
            }
        }
        JavaRDD<Row> rowsRDD = sc.parallelize(rows);

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("city_id", DataTypes.StringType, true)));

        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowsRDD, schema);

        dataFrame.registerTempTable("user_visit_action");
        dataFrame.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {

                log.info("--------------" + row);
            }
        });
        /**
         * ==================================================================
         */

        rows.clear();
        String[] sexes = new String[]{"male", "female"};
        for (int i = 0; i < 100; i++) {
            long userid = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userid, username, name, age,
                    professional, city, sex);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)));

        Dataset<Row> df2 = sqlContext.createDataFrame(rowsRDD, schema2);

        df2.registerTempTable("user_info");
        df2.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                log.info("--------------" + row);
            }
        });

        rows.clear();


        //mock product_info 表
        int[] productStatus = new int[]{0, 1};
        for (int i = 0; i < 100; i++) {
            long productid = i;
            String productName = "product" + i;
            String extendinfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";
            Row row = RowFactory.create(productid, productName, extendinfo);
            rows.add(row);
        }

        rowsRDD =sc.parallelize(rows);

        StructType schema3 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.LongType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)));
        Dataset<Row> df3 = sqlContext.createDataFrame(rowsRDD, schema3);

        df3.registerTempTable("product_info");
        df3.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                log.info("--------------" + row);
            }
        });
        rows.clear();
    }

}
