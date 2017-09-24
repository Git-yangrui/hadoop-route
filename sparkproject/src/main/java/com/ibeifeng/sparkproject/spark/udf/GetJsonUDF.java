package com.ibeifeng.sparkproject.spark.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

public class GetJsonUDF implements UDF2<String, String, String> {

    @Override
    public String call(String s, String field) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(s);
            return jsonObject.getString(field);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
