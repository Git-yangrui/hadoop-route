package com.yangrui.hadoop.partitioner;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.yangrui.hadoop.flowcount.FlowCountBean;

public class FlowCountPartition {
	public static class FlowCountPartitionMapper extends Mapper<LongWritable, Text, Text, FlowCountBean> {
		private FlowCountBean countBean = new FlowCountBean();

		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			String[] splits = StringUtils.split(lineValue, "\t");
			String phoneNum = splits[1];
			long up_flow = Long.parseLong(splits[(splits.length - 3)]);
			long dw_flow = Long.parseLong(splits[(splits.length - 2)]);
			this.countBean.set(phoneNum, up_flow, dw_flow);
			context.write(new Text(phoneNum), this.countBean);
		}
	}

	public static class FlowCountPartitiReducer extends Reducer<Text, FlowCountBean, Text, FlowCountBean> {
		private FlowCountBean countBean = new FlowCountBean();

		protected void reduce(Text key, Iterable<FlowCountBean> values,
				Context context)
				throws IOException, InterruptedException {
			long up_flow_sum = 0L;
			long dw_flow_sum = 0L;
			for (FlowCountBean flowCountBean : values) {
				up_flow_sum += flowCountBean.getUp_flow();
				dw_flow_sum += flowCountBean.getDw_flow();
			}
			this.countBean.set(key.toString(), up_flow_sum, dw_flow_sum);
			context.write(key, this.countBean);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job instance = Job.getInstance(configuration);
		instance.setJarByClass(FlowCountPartition.class);
		instance.setMapperClass(FlowCountPartitionMapper.class);
		instance.setReducerClass(FlowCountPartitiReducer.class);

		instance.setPartitionerClass(ArePartioner.class);

		instance.setNumReduceTasks(5);
		instance.setMapOutputKeyClass(Text.class);
		instance.setMapOutputValueClass(FlowCountBean.class);
		instance.setOutputKeyClass(Text.class);
		instance.setOutputValueClass(FlowCountBean.class);
		instance.setInputFormatClass(TextInputFormat.class);
		instance.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(instance, new Path[] { new Path(args[0]) });
		FileOutputFormat.setOutputPath(instance, new Path(args[1]));
		instance.waitForCompletion(true);
	}
}
