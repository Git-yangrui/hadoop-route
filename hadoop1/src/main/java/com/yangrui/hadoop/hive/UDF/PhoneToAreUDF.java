package com.yangrui.hadoop.hive.UDF;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

public class PhoneToAreUDF extends UDF {
	private static Map<String, String> areMap = new HashMap<String, String>();
	static {
		areMap.put("136", "beijing");
		areMap.put("137", "tianji");
		areMap.put("138", "nanjing");
		areMap.put("139", "shanghai");
		areMap.put("188", "toyoko");
	}

	// 手机号 -----城市名
	public  String evaluate(String phoneNum) {
		String area = areMap.get(phoneNum.substring(0, 3));
		return area==null?"huoxing":area;
	}

	// sum
	public  int evaluate(int up_flow,int dw_flow) {
      return up_flow+dw_flow;
	}
}
