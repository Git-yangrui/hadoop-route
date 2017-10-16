package com.yangrui.mrdesinmode.composite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CompositeJoinTestDriver extends Configured implements Tool {

	@Override
    public int run(String[] args) throws Exception {
		Path inputAPath = new Path("C:\\composite\\file1");
		Path inputBPath = new Path("C:\\composite\\file2");
		Path outputDir = new Path("C:\\composite\\out4");
		String joinType ="inner11";

		if (!(joinType.equalsIgnoreCase("inner") || joinType
				.equalsIgnoreCase("outer"))) {
			System.err.println("Join type not set to inner or outer");
			System.exit(2);
		}

		JobConf conf = new JobConf(new Configuration(),
				CompositeJoinTestDriver.class);

		conf.setJobName(this.getClass().getName());
		conf.setJarByClass(this.getClass());

		conf.setMapperClass(CompositeJoinMapper.class);
		conf.setNumReduceTasks(0);

		conf.setInputFormat(CompositeInputFormat.class);
		conf.set("mapred.join.expr", CompositeInputFormat.compose(joinType,
				KeyValueTextInputFormat.class, inputAPath, inputBPath));
		TextOutputFormat.setOutputPath(conf, outputDir);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		RunningJob job = JobClient.runJob(conf);

		while (!job.isComplete()) {
			Thread.sleep(1000);
		}

		return job.isSuccessful() ? 0 : 2;
	}

	public static void main(String[] args) throws Exception {

		System.setProperty("HADOOP_HOME","D:\\hadoop-2.5.0");
		int exitCode = ToolRunner.run(new CompositeJoinTestDriver(), args);
		System.exit(exitCode);
	}
}