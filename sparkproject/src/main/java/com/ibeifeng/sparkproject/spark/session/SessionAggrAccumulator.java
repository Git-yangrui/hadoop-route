package com.ibeifeng.sparkproject.spark.session;

import com.ibeifeng.sparkproject.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionAggrAccumulator extends AccumulatorV2<String, String> {

    private Logger log = LoggerFactory.getLogger(SessionAggrAccumulator.class);
    private String initValue =
            Constants.SESSION_COUNT + "=0|"
                    + Constants.TIME_PERIOD_1s_3s + "=0|"
                    + Constants.TIME_PERIOD_4s_6s + "=0|"
                    + Constants.TIME_PERIOD_7s_9s + "=0|"
                    + Constants.TIME_PERIOD_10s_30s + "=0|"
                    + Constants.TIME_PERIOD_30s_60s + "=0|"
                    + Constants.TIME_PERIOD_1m_3m + "=0|"
                    + Constants.TIME_PERIOD_3m_10m + "=0|"
                    + Constants.TIME_PERIOD_10m_30m + "=0|"
                    + Constants.TIME_PERIOD_30m + "=0|"
                    + Constants.STEP_PERIOD_1_3 + "=0|"
                    + Constants.STEP_PERIOD_4_6 + "=0|"
                    + Constants.STEP_PERIOD_7_9 + "=0|"
                    + Constants.STEP_PERIOD_10_30 + "=0|"
                    + Constants.STEP_PERIOD_30_60 + "=0|"
                    + Constants.STEP_PERIOD_60 + "=0";


    @Override
    public boolean isZero() {
        return true;
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrAccumulator accumulator = new SessionAggrAccumulator();
        accumulator.initValue = this.initValue;
        return accumulator;
    }

    @Override
    public void reset() {
        String resetValue =
                Constants.SESSION_COUNT + "=0|"
                        + Constants.TIME_PERIOD_1s_3s + "=0|"
                        + Constants.TIME_PERIOD_4s_6s + "=0|"
                        + Constants.TIME_PERIOD_7s_9s + "=0|"
                        + Constants.TIME_PERIOD_10s_30s + "=0|"
                        + Constants.TIME_PERIOD_30s_60s + "=0|"
                        + Constants.TIME_PERIOD_1m_3m + "=0|"
                        + Constants.TIME_PERIOD_3m_10m + "=0|"
                        + Constants.TIME_PERIOD_10m_30m + "=0|"
                        + Constants.TIME_PERIOD_30m + "=0|"
                        + Constants.STEP_PERIOD_1_3 + "=0|"
                        + Constants.STEP_PERIOD_4_6 + "=0|"
                        + Constants.STEP_PERIOD_7_9 + "=0|"
                        + Constants.STEP_PERIOD_10_30 + "=0|"
                        + Constants.STEP_PERIOD_30_60 + "=0|"
                        + Constants.STEP_PERIOD_60 + "=0";
        initValue = resetValue;
    }

    @Override
    public void add(String v) {
        String v1=initValue;
        String v2=v;
        if(StringUtils.isNotEmpty(v1)&&StringUtils.isNotEmpty(v2)){
            String newResult="";
            String oldValue=com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(v1,"\\|",v2);
            if(oldValue!=null){
                String newValue=String.valueOf(Integer.valueOf(oldValue)+1);
                newResult  = com.ibeifeng.sparkproject.util.StringUtils.setFieldInConcatString(v1, "\\|", v2, newValue);
            }
            initValue = newResult;
        }
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        if(other!=null){
            initValue=other.value();
        }
    }

    @Override
    public String value() {
        return initValue;
    }
}
