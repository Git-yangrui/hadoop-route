package com.yangrui.hadoop.ii;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 *
 */

public class InverseIndexStepOne {

	public static class InverseIndexStepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text k = new Text();
		private LongWritable v = new LongWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String[] words = StringUtils.split(line, " ");

			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String fileName = inputSplit.getPath().getName();

			// < hello-->a.txt , 1>
			for (String word : words) {
				k.set(word + "-->" + fileName);
				v.set(1);
				context.write(k, v);

			}

		}

	}

	public static class InverseIndexStepOneReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable v = new LongWritable();

		// <hello-->a.txt ,{1,1,1...}>
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			long count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			v.set(count);
			context.write(key, v);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job_stepOne = Job.getInstance(conf);

		job_stepOne.setJarByClass(InverseIndexStepOne.class);

		job_stepOne.setMapperClass(InverseIndexStepOneMapper.class);
		job_stepOne.setReducerClass(InverseIndexStepOneReducer.class);

		job_stepOne.setOutputKeyClass(Text.class);
		job_stepOne.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job_stepOne, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_stepOne, new Path(args[1]));

		job_stepOne.waitForCompletion(true);

	}

}
