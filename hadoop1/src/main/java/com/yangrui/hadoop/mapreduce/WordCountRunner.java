package com.yangrui.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用来描述一个作业job
 * 
 * @author yangrui
 *
 */
public class WordCountRunner {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job wcJob = Job.getInstance(conf);
        
        wcJob.setJarByClass(WordCountRunner.class);
        
        wcJob.setMapperClass(MyWordCountMap.class);
        wcJob.setReducerClass(MyWordCountReduce.class);
        
        wcJob.setMapOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(LongWritable.class);
        
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(LongWritable.class);
        
        //指定原始路径及文件地址
        FileInputFormat.setInputPaths(wcJob, "hdfs://192.168.1.100:9000/src");
        //输出路径
        FileOutputFormat.setOutputPath(wcJob, new Path("hdfs://192.168.1.100:9000/output"));
        
        wcJob.waitForCompletion(true);
//        boolean waitForCompletion = wcJob.waitForCompletion(true);
        
//        System.exit(waitForCompletion?0:1);
	}
}
