package com.yangrui.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HBaseOperation {

    public static void main(String[] args) throws IOException {
        String usertable="user";
        HTable hBaseTable = getHBaseTable(usertable);
        Get get=new Get(Bytes.toBytes("100001"));
        Result result = hBaseTable.get(get);
        for (Cell cell:result.rawCells()) {
            System.out.println(new String(cell.getFamily()));
            System.out.println(new String(cell.getQualifier()));
            System.out.println(new String(cell.getValue()));
            System.out.println(new String(CellUtil.cloneFamily(cell)));
        }

//        hBaseTable.ba


    }

    private static HTable getHBaseTable(String usertable) throws IOException {

        Scan scan=new Scan();

        Filter filter;
        Configuration configuration = HBaseConfiguration.create();
        return new HTable(configuration,usertable);
    }
}