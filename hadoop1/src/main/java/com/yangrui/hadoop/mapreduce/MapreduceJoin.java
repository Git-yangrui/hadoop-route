package com.yangrui.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;





//     customer
//        客户编号	姓名	地址	电话
//        1	hanmeimei	ShangHai	110
//        2	leilei	BeiJing	112
//        3	lucy	GuangZhou	119
//     ** order**
//        订单编号	客户编号	其它字段被忽略
//        1	1	50
//        2	1	200
//        3	3	15
//        4	3	350
//        5	3	58
//        6	1	42
//        7	1	352
//        8	2	1135
//        9	2	400
//        10	2	2000
//        11	2	300
//        要求对customer和orders按照客户编号进行连接，结果要求对客户编号分组，对订单编号排序，对其它字段不作要求
//     客户编号	订单编号	订单金额	姓名	地址	电话
//        1	1	50	hanmeimei	ShangHai	110
//        1	2	200	hanmeimei	ShangHai	110
//        1	6	42	hanmeimei	ShangHai	110
//        1	7	352	hanmeimei	ShangHai	110
//        2	8	1135	leilei	BeiJing	112
//        2	9	400	leilei	BeiJing	112
//        2	10	2000	leilei	BeiJing	112
//        2	11	300	leilei	BeiJing	112
//        3	3	15	lucy	GuangZhou	119
//        3	4	350	lucy	GuangZhou	119
//        3	5	58	lucy	GuangZhou	119


//大表  小表
public class MapreduceJoin extends Configured implements Tool {

    private static final String CUSTOMER_CACHE_URL = "hdfs://hadoop1:9000/user/hadoop/mapreduce/cache/customer.txt";

    private static class CustomerBean{
        private int custId;

        private String name;

        private String address;

        private String phone;

        public CustomerBean(){


        }

        public CustomerBean(int custId, String name, String address, String phone) {
            this.custId = custId;
            this.name = name;
            this.address = address;
            this.phone = phone;
        }

        public int getCustId() {
            return custId;
        }

        public void setCustId(int custId) {
            this.custId = custId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
        }
    }

    private static class CustOrderMapOutKey implements WritableComparable<CustOrderMapOutKey>{
        private int custId;

        private int orderId;

        public int getCustId() {
            return custId;
        }

        public void setCustId(int custId) {
            this.custId = custId;
        }

        public int getOrderId() {
            return orderId;
        }

        public void setOrderId(int orderId) {
            this.orderId = orderId;
        }

        public void set(int custId,int orderId){
            this.custId=custId;
            this.orderId=orderId;

        }

        @Override
        public int compareTo(CustOrderMapOutKey o) {
            if(this.getCustId()>=o.getCustId()){
                return 1;
            }

            if(this.getOrderId()>=o.getOrderId()){
                return 1;
            }
            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

            dataOutput.writeInt(custId);
            dataOutput.writeInt(orderId);

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            custId= dataInput.readInt();
            orderId=dataInput.readInt();
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CustOrderMapOutKey)) return false;
            CustOrderMapOutKey that = (CustOrderMapOutKey) o;
            return getCustId() == that.getCustId() && getOrderId() == that.getOrderId();
        }

        @Override
        public int hashCode() {
            int result = getCustId();
            result = 31 * result + getOrderId();
            return result;
        }

        @Override
        public String toString() {
            return custId+"\t"+orderId;
        }
    }

  private static class JoinMapper extends Mapper<LongWritable,Text,CustOrderMapOutKey,Text>{
       private final CustOrderMapOutKey outputKey=new CustOrderMapOutKey();

       private final Text outputValue=new Text();

      /**
       * 在内存中customer数据
       */
      private static final Map<Integer,CustomerBean> CUSTOMER_MAP = new HashMap();

      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String[] split = value.toString().split("\t");

          if(split.length<3){
              return;
          }

          int custId=Integer.parseInt(split[1]);

          CustomerBean customerBean = CUSTOMER_MAP.get(custId);

          if(customerBean==null){
              return;
          }


          StringBuffer sb=new StringBuffer();


          sb.append(split[2])
                  .append("\t")
                  .append(customerBean.getName())
                  .append("\t")
                  .append(customerBean.getAddress())
                  .append("\t")
                  .append(customerBean.getPhone());


          outputValue.set(sb.toString());
          outputKey.set(custId, Integer.parseInt(split[0]));

          context.write(outputKey,outputValue);

      }

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
          FileSystem fs=FileSystem.get(URI.create(CUSTOMER_CACHE_URL),context.getConfiguration());

          FSDataInputStream open = fs.open(new Path(CUSTOMER_CACHE_URL));


          BufferedReader reader=new BufferedReader(new InputStreamReader(open));
          String line=null;
          
          String []cols=null;
          while((line=reader.readLine())!=null){
              cols = line.split("\t");

              CustomerBean bean=new CustomerBean(Integer.parseInt(cols[0]), cols[1], cols[2], cols[3]);
              CUSTOMER_MAP.put(bean.getCustId(),bean);

          }
          
          
      }
  }
  private static class JoinReducer extends Reducer<CustOrderMapOutKey,Text,CustOrderMapOutKey,Text>{
      @Override
      protected void reduce(CustOrderMapOutKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text text:values){
            context.write(key,text);
        }
      }
  }

    @Override
    public int run(String[] strings) throws Exception {


        Configuration conf = getConf();

        Job job= Job.getInstance(conf,MapreduceJoin.class.getSimpleName());
//        bloomf
        job.setJarByClass(MapreduceJoin.class);

//        job.setJar();

        job.addCacheFile(URI.create(CUSTOMER_CACHE_URL));

        FileInputFormat.addInputPath(job,new Path(""));

        FileOutputFormat.setOutputPath(job,new Path(""));

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(CustOrderMapOutKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(CustOrderMapOutKey.class);
        job.setOutputValueClass(Text.class);

        boolean res = job.waitForCompletion(true);

        return res ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            new IllegalArgumentException("Usage: ");
            return;
        }

        ToolRunner.run(new Configuration(), new MapreduceJoin(), args);
    }
}
