package com.yangrui.hadooproute;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class RemoveYinhaoUDF extends UDF {

    public Text evaluate(Text input){
        if (null == input) {
            return new Text();
        }
        return new Text(input.toString().replaceAll("\"",""));
    }

}
