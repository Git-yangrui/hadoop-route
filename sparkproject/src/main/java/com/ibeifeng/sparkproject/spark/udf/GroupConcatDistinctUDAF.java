package com.ibeifeng.sparkproject.spark.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{

    //输入数据
    private StructType inputSchema= DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("cityInfo",DataTypes.StringType,true)));

    //缓冲数据
    private StructType bufferSchema= DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("bufferCityInfo",DataTypes.StringType,true)));

    //制定返回的类型   string类型
    private DataType dataType=DataTypes.StringType;

    //指定是否确定还行
    private boolean deterministic=true;


    @Override
    public StructType inputSchema() {


        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
         //初始的值
        buffer.update(0,"");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        String  bufferCityInfo = buffer.getString(0);
        String cityInfo = input.getString(0);

        if(!bufferCityInfo.contains(cityInfo)){
            if("".equals(bufferCityInfo)){
                bufferCityInfo += cityInfo;

            }else {
                bufferCityInfo+=","+cityInfo;
            }

            buffer.update(0,bufferCityInfo);
        }
    }
     //各个节点进行merge
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String buffcityInfo1 = buffer1.getString(0);
        String buffcityInfo2 = buffer2.getString(0);
        for(String cityinfo:buffcityInfo1.split("\\,")){
            if(!buffcityInfo1.contains(cityinfo)){
                if("".equals(buffcityInfo1)){
                    buffcityInfo1 += cityinfo;

                }else {
                    buffcityInfo1+=","+cityinfo;
                }
            }


        }

        buffer1.update(0,buffcityInfo1);


    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
