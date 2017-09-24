package com.ibeifeng.sparkproject.spark.udf;

import org.apache.spark.sql.api.java.UDF3;

public class ContactLongStringUDF implements UDF3<Long,String,String,String>{

    @Override
    public String call(Long v1, String v2, String s2) throws Exception {
        return String.valueOf(v1)+s2+v2;
    }
}
