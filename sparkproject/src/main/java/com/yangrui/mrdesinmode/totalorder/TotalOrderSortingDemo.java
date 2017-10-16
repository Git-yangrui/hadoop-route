package com.yangrui.mrdesinmode.totalorder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;


public class TotalOrderSortingDemo extends Configured implements Tool {

   public static class LastAccessDataMapper extends Mapper<Object,Text,Text,Text>{

       private Text outKey=new Text();
       private Text outValue=new Text();
       @Override
       protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {


           String[] split = value.toString().split("\t");

//           Map<String, String> stringStringMap = MRDPUtils.transformXmlToMap(value.toString());
//           String lastAccessDate = stringStringMap.get("LastAccessDate");
           outKey.set(split[0]);
           outValue.set(split[1]);
           context.write(outKey,outValue);
       }
   }



   public static class ValueReducer extends Reducer<Text,Text,Text,NullWritable>{

       @Override
       protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          for(Text text:values){


              context.write(text,NullWritable.get());
          }
       }
   }
    @Override
    public int run(String[] strings) throws Exception {

        String hadoop_home = System.getProperty("HADOOP_HOME");

        System.setProperty("HADOOP_USER_NAME", "yangrui");

        Configuration conf = getConf();


        Job job1=Job.getInstance(conf,"job1");
        Path inputPath=new Path("C:\\text");
        Path outpath=new Path("C:\\out");

        Path partitionFile=new Path(outpath,"_partions.lst");

        Path outputStage=new Path(outpath,"_staging");

        Path outputorder=outpath;

        job1.setJarByClass(TotalOrderSortingDemo.class);
        job1.setNumReduceTasks(0);


        TextInputFormat.setInputPaths(job1,inputPath);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job1,outputStage);


       return  Integer.valueOf(job1.waitForCompletion(true)==true?0:1);
    }


    public static void main(String[] args)  throws Exception {
//        ToolRunner.run(new Configuration(),new TotalOrderSortingDemo(),args);
          int a=0;
          int b=0;
        if(a<1){
            System.out.println("111111");
        }else if(b<1){
            System.out.println("22222222211111");
        }

//        CompositeInputFormat.compose()
    }
}
