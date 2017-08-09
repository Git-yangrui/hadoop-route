package com.yangrui.hadoop.flowcountsort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.yangrui.hadoop.flowcount.*;

public class FlowCountSort
{
  public static class FlowCountSortMapper
    extends Mapper<LongWritable, Text, FlowCountBean, NullWritable>
  {
    private FlowCountBean bean = new FlowCountBean();
    
    protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException
    {
      String line = value.toString();
      String[] split = StringUtils.split(line, "\t");
      String phonenum = split[0];
      long up_flow = Long.parseLong(split[1]);
      long dw_flow = Long.parseLong(split[2]);
      this.bean.set(phonenum, up_flow, dw_flow);
      context.write(this.bean, NullWritable.get());
    }
  }
  
  public static class FlowCountSortReduce
    extends Reducer<FlowCountBean, NullWritable, Text, FlowCountBean>
  {
    protected void reduce(FlowCountBean bean, Iterable<NullWritable> values, Context arg2)
      throws IOException, InterruptedException
    {
      arg2.write(new Text(bean.getPhoneNum()), bean);
    }
  }
  
  public static void main(String[] args)
    throws Exception
  {
    Configuration configuration = new Configuration();
    Job instance = Job.getInstance(configuration);
    instance.setJarByClass(FlowCountSort.class);
    instance.setMapperClass(FlowCountSortMapper.class);
    instance.setReducerClass(FlowCountSortReduce.class);
    instance.setMapOutputKeyClass(FlowCountBean.class);
    instance.setMapOutputValueClass(NullWritable.class);
    instance.setOutputKeyClass(Text.class);
    instance.setOutputValueClass(FlowCountBean.class);
    instance.setInputFormatClass(TextInputFormat.class);
    instance.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.setInputPaths(instance, new Path[] { new Path(args[0]) });
    FileOutputFormat.setOutputPath(instance, new Path(args[1]));
    instance.waitForCompletion(true);
  }
}
