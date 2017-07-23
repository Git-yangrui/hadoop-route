package com.yangrui.hadooproute;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import sun.applet.Main;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 处理日期类型
 */

public class DateFormatUDF extends UDF {
    private final SimpleDateFormat inputDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);

    private final SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.ENGLISH);

    public Text evaluate(Text input) {
        Text oput = new Text();
        if (null == input) {
            return null;
        }
        String inoutDate = input.toString().trim();
        try {
            Date parse = inputDateFormat.parse(inoutDate);
            String formatDateToString = outputDateFormat.format(parse);
            oput.set(formatDateToString);
        } catch (ParseException e) {
            e.printStackTrace();
        }finally {
            return oput;
        }
    }

    public static void main(String[] args) {
        Text evaluate = new DateFormatUDF().evaluate(new Text("31/Aug/2015:00:04:37 +0800"));
        System.out.println(evaluate.toString());
    }
}
