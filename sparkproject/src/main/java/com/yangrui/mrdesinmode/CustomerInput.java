package com.yangrui.mrdesinmode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CustomerInput extends Configured implements Tool {

    public static final String NUM_MAP_TASKS = "random.generator.map.tasks";

    public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";

    public static final String RADOM_WORD_LIST = "random.generator.random.word.file";

    public static class CustomerFileSplit extends InputSplit implements Writable {

        public CustomerFileSplit() {

        }

        @Override
        public void write(DataOutput out) throws IOException {

        }

        @Override
        public void readFields(DataInput in) throws IOException {

        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }
    }

    public static class CustomerRecordReader extends RecordReader<Text, NullWritable> implements Serializable {

        private int numRecordsForPerTasksToCreate = 0;

        private int createdAlreadyRecordsCount = 0;

        private Text key = new Text();

        private NullWritable nullValue = NullWritable.get();

        private Random random = new Random();

        private ArrayList<String> randomWords = new ArrayList<>();

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {

            this.numRecordsForPerTasksToCreate = taskAttemptContext.getConfiguration().getInt(NUM_RECORDS_PER_TASK, -1);

            Path[] localCacheFiles = taskAttemptContext.getLocalCacheFiles();

            BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\yangrui\\Desktop\\cache.txt"));

            String line = null;
            while ((line = reader.readLine()) != null) {
                randomWords.add(line);
            }

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (createdAlreadyRecordsCount < numRecordsForPerTasksToCreate) {
                int userid = random.nextInt(1000000000);
                int scoreForMath = random.nextInt(100);
                int scoreForEnglish= random.nextInt(100);
                String subject1 = "Math";

                String subject2 = "English";

                String textDecriotionForMath=getRandomText();

                String textDecriotionForEnglish=getRandomText();

                String record=String.valueOf(userid)+"\t"+subject1+":"+scoreForMath+":"+textDecriotionForMath
                        +"\t"+subject2+":"+scoreForEnglish+":"+textDecriotionForEnglish;
                key.set(record);

                ++createdAlreadyRecordsCount;
                return true;
            }else{
                return false;
            }

        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return nullValue;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return createdAlreadyRecordsCount/numRecordsForPerTasksToCreate;
        }

        @Override
        public void close() throws IOException {

        }

        private String getRandomText(){
            int i = random.nextInt(randomWords.size());
            return randomWords.get(i);
        }
    }

    public static class CustomerFileInputFormat extends InputFormat<Text, NullWritable> {

        @Override
        public RecordReader<Text, NullWritable>
        createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {

            CustomerRecordReader customerReader=new CustomerRecordReader();
            customerReader.initialize(inputSplit,taskAttemptContext);
            return customerReader;
        }

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {

            int mapTaskNum = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
            ArrayList<InputSplit> splits = new ArrayList<>();
            for (int i = 0; i < mapTaskNum; i++) {
                splits.add(new CustomerFileSplit());
            }
            return splits;
        }

        public static void setNumMapTasks(Job job,int nums){
            job.getConfiguration().setInt(NUM_MAP_TASKS,nums);
        }

        public static void setNumRecordsForPerTask(Job job,int nums){
            job.getConfiguration().setInt(NUM_RECORDS_PER_TASK,nums);
        }

        public static void setRandomWordList(Job job,Path file) throws  Exception{

            job.addFileToClassPath(file);

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        int numMapTask=Integer.parseInt(args[0]);
        int numRecordsForPerTask=Integer.parseInt(args[1]);
        Path outputDir=new Path(args[2]);
        Job job=Job.getInstance(getConf(),"CustomerJob");
//        job.addFileToClassPath());
//        job.addCacheFile(new URI());
        job.setJarByClass(CustomerInput.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(CustomerFileInputFormat.class);
        CustomerFileInputFormat.setNumMapTasks(job,numMapTask);
        CustomerFileInputFormat.setNumRecordsForPerTask(job,numRecordsForPerTask);
        CustomerFileInputFormat.setRandomWordList(job,new Path(args[3]));
        TextOutputFormat.setOutputPath(job,outputDir);
        job.setMapperClass(CutomerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(CustomerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        int i = job.waitForCompletion(true) ? 0 : 1;

        return i;
    }

    private void check(){

    }

    public static class CutomerMapper extends Mapper<Object,NullWritable,Text,NullWritable>{
        private Text outkey=new Text();

        @Override
        protected void map(Object key, NullWritable value, Context context) throws IOException, InterruptedException {
            outkey.set(key.toString());
            context.write(outkey,value);
        }
    }

    public static class CustomerReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
          context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new CustomerInput(), args);
        System.exit(run);
    }
}
