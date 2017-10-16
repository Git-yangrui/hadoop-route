package com.yangrui.mrdesinmode.mrpartitioner;

import com.yangrui.mrdesinmode.MRDPUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;


public class CustomerPartitionerProccess extends Configured implements Tool {


    public static class LastAccessDateMapper extends Mapper<Object, Text, IntWritable, Text> {
         private MultipleOutputs mos;
        private final static SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        private IntWritable outKey = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos=new MultipleOutputs(context);

//            mos.wri
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsedContext = MRDPUtils.transformXmlToMap(value.toString());
            String lastAccessDateContext = parsedContext.get("LastAccessDate");

            if (lastAccessDateContext == null) {
                return;
            }

            Calendar instance = Calendar.getInstance();

            try {
                instance.setTime(FORMAT.parse(lastAccessDateContext));
            } catch (ParseException e) {
                throw new InterruptedException("parsed date failed ");
            }
            outKey.set(instance.get(Calendar.YEAR));
            context.write(outKey, value);
        }
    }

    //K IntWritable  V Text
    public static class CustomerPartitioner extends Partitioner<IntWritable, Text> implements Configurable {

        private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";

        private Configuration conf = null;

        private int minLastAccessDateYear = 0;


        @Override
        public Configuration getConf() {

            return conf;
        }

        @Override
        public void setConf(Configuration configuration) {
            this.conf = configuration;

            minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
        }

        @Override
        public int getPartition(IntWritable k, Text v, int i) {

            return k.get() - minLastAccessDateYear;
        }

        public static void setMinLastAccessDate(Job job, int minLastAccessDateYear) {

            job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, minLastAccessDateYear);
        }
    }

    public static class VlaueReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {

                context.write(t, NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, CustomerPartitionerProccess.class.getSimpleName());
        job.setJarByClass(CustomerPartitionerProccess.class);
        FileInputFormat.addInputPath(job, new Path(""));

        FileOutputFormat.setOutputPath(job, new Path(""));

        job.setMapperClass(LastAccessDateMapper.class);

        job.setPartitionerClass(CustomerPartitioner.class);
        CustomerPartitioner.setMinLastAccessDate(job, 2013);
        job.setReducerClass(VlaueReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(4);
        boolean res = job.waitForCompletion(true);

        return res ? 0 : 1;




    }


    public static void main(String[] args)  throws Exception{

        ToolRunner.run(new Configuration(),new CustomerPartitionerProccess(),args);
    }
}
