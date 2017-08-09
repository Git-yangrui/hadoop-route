package com.yangrui.hadoop.hive.UDF;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class LowerUDF extends UDF {
	
	public Text evaluate(Text str) {
		
		if(StringUtils.isNotEmpty(str.toString())){
			
		}
		return new Text("22");
	}
}
