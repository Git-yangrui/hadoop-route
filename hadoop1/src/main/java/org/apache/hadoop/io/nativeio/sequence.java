package org.apache.hadoop.io.nativeio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Random;

public class sequence {
    /**
     * @param args
     */

    public static final String Output_path="/home/hadoop/test/A.txt";
    public static Random random=new Random();
    private static final String[] DATA={

          "Five,six,pick up sticks",
            "Three,four,shut the door",
            "Three,four,shut the door",
          "Seven,eight,lay them straight",
            "One,two,buckle my shoe",
          "Nine,ten,a big fat hen"
         };
    public static Configuration conf=new Configuration();
    public static void write(String pathStr) throws IOException{
        Path path=new Path(pathStr);
        LocalFileSystem local = FileSystem.getLocal(new Configuration());

        SequenceFile.Writer writer=SequenceFile.createWriter(local, conf, path, Text.class, IntWritable.class);
        Text key=new Text();
        IntWritable value=new IntWritable();
        for(int i=0;i<DATA.length;i++){
            key.set(DATA[i]);
            value.set(random.nextInt(10));
            System.out.println("key：："+key);
            System.out.println("value=="+value);

            System.out.println(writer.getLength());
            writer.append(key, value);
           
        }
        writer.close();
    }
    public static void read(String pathStr) throws IOException{
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        SequenceFile.Reader reader=new SequenceFile.Reader(local, new Path(pathStr), conf);
        Text key=new Text();
        IntWritable value=new IntWritable();
        while(reader.next(key, value)){
            System.out.println(key);
            System.out.println(value);
        }
    }
   
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        write(Output_path);
        read(Output_path);
    }   
}