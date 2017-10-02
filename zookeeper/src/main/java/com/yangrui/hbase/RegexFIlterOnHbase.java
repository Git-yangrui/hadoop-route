package com.yangrui.hbase;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;

public class RegexFIlterOnHbase {

    public static void main(String[] args) {
        Filter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(".*_20150681"));
        Scan scan=new Scan();
        scan.setFilter(filter);


    }
}
