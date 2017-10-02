package com.ibeifeng.sparkproject.util;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

public class JsonObjectTest {

    @Test
    public void test_Jsonobject(){
//        {"startAge":"10","endAge":"50"}

        JSONObject jsonObject=JSONObject.parseObject(" {\"startAge\":[\"10\"],\"endAge\":[\"50\"]}");
        Object startAge = jsonObject.get("startAge");
        jsonObject.getJSONArray("startAge");

//        Serializable
    }
}
