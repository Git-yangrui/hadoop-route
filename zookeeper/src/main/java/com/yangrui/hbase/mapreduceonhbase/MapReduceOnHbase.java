package com.yangrui.hbase.mapreduceonhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MapReduceOnHbase extends Configured implements Tool {

    public static class ReadUserMapper extends TableMapper<Text,Put>{

        Text outputkey=new Text();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {
            //get row key
            String rowkey= Bytes.toString(key.get());
            outputkey.set(rowkey);
            Put put=new Put(key.get());
            String coumlnVaule= Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("colmun")));
            String coumln1Vaule= Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("colmun1")));
            String coumln2Vaule= Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("colmun2")));
            String coumln3Vaule= Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("colmun3")));
            String coumln4Vaule= Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("colmun4")));
            String coumln5Vaule= Bytes.toString(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("colmun5")));

            //如果从文件里面读取则要准备put对象
          context.write(outputkey,put);
        }
    }



    public static class WriteUserReduce extends TableReducer<Text,Put,ImmutableBytesWritable>{

        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context)
              throws IOException, InterruptedException {
              values.forEach(each->{

                  try {
                      context.write(null,each);
                  } catch (IOException e) {
                      e.printStackTrace();
                  } catch (InterruptedException e) {
                      e.printStackTrace();
                  }
              });
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job=Job.getInstance(this.getConf(),this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Scan scan=new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);


        TableMapReduceUtil.initTableMapperJob("user",scan,ReadUserMapper.class,Text.class,Put.class,job);
        TableMapReduceUtil.initTableReducerJob("user",WriteUserReduce.class,job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration= HBaseConfiguration.create();
        ToolRunner.run(configuration,new MapReduceOnHbase(),args);
        System.exit(1);
    }
}
