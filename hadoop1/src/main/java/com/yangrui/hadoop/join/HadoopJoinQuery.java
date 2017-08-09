package com.yangrui.hadoop.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HadoopJoinQuery {
	private static final Log LOG = LogFactory.getLog(Job.class);
	public static class JoinQueryMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = StringUtils.split(line, "|");
			LOG.info("split"+split);
			LOG.info("split.length"+split.length);
			String id = split[0];
			String name = split[1];
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String filename = inputSplit.getPath().getName();
			k.set(id);
			v.set(name + "-->" + filename);
			context.write(k, v);
		}
	}
	
	public static class JoinQueryReducer extends Reducer<Text, Text, Text, Text>{
		private List<String> listRight=null;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String leftkey="";
			// key value  k:001   v:  iphone6-->a  00101-->b 00002-->b
			listRight=new ArrayList<String>();
			for (Text text : values) {
				if(text.toString().contains("a.txt")){
					String[] split = StringUtils.split(text.toString(), "-->");
					leftkey = split[0];
				}else{
					listRight.add(text.toString());
				}
			}
			
			for (String text : listRight) {
					String result="";
					String[] split = StringUtils.split(text.toString(), "-->");
					result += leftkey+"\t"+split[0];
					context.write(key, new Text(result));
			}
			listRight=null;
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "root"); 
	    Job instance = Job.getInstance(configuration);
	    instance.setJarByClass(HadoopJoinQuery.class);
	    instance.setMapperClass(JoinQueryMapper.class);
	    instance.setReducerClass(JoinQueryReducer.class);
	    instance.setMapOutputKeyClass(Text.class);
	    instance.setMapOutputValueClass(Text.class);
	    instance.setOutputKeyClass(Text.class);
	    instance.setOutputValueClass(Text.class);
	    instance.setInputFormatClass(TextInputFormat.class);
	    instance.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(instance, new Path[] { new Path(args[0]) });
	    FileOutputFormat.setOutputPath(instance, new Path(args[1]));
	    instance.waitForCompletion(true);
	}
}
