package com.ibeifeng.sparkproject.json;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

public class JsonTest {

    @Test
    public void test_Json(){

        JSONObject jsonObject=JSONObject.parseObject("{\"yangyang\":\"18\",\"mingzhu\":\"20\"}");
        String yangyang = jsonObject.getString("yangyang");
        System.out.println(yangyang);
    }
}
