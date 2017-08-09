package com.yangrui.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
/**
 * 
 * @author yangrui
 *
 */
public class MyWordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
        String[] splitS = StringUtils.split(line, " ");
        for (String word : splitS) {
			context.write(new Text(word), new LongWritable(1));
		}
	}
}
