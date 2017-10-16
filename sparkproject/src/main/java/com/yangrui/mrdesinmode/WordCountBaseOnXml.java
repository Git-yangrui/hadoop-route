package com.yangrui.mrdesinmode;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class WordCountBaseOnXml extends Configured implements Tool {

    private final static String TEXTKEY="Text";

    public static class WordCountMapper extends Mapper<Object,Text,Text,IntWritable>{
        private final static IntWritable one=new IntWritable(1);

        private Text word=new Text();

//        private final static String TEXTKEY="Text";

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
            String txtContext = parsed.get(WordCountBaseOnXml.TEXTKEY);

            if(txtContext==null){
                return;
            }
            txtContext=StringEscapeUtils.unescapeHtml4(txtContext.toLowerCase());
            txtContext=txtContext.replaceAll("'","");
            txtContext=txtContext.replaceAll("[^a-zA-Z]"," ");


            StringTokenizer itr=new StringTokenizer(txtContext);

            while(itr.hasMoreTokens()){


                word.set(itr.nextToken());

                context.write(word,one);
            }

        }
    }


    public static class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{

        private IntWritable result=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
               int sum=0;


               for(IntWritable oneEach:values){
                   sum+=oneEach.get();
               }

               result.set(sum);


            context.write(key,result);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }
}
