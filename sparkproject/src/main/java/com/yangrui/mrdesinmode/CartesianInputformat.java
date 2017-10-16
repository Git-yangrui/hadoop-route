package com.yangrui.mrdesinmode;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit;

import java.io.IOException;
import java.util.List;

public class CartesianInputformat extends FileInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        CompositeInputSplit s=new CompositeInputSplit();

//       FileInputFormat inputFormat=re
        return super.getSplits(job);
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}
