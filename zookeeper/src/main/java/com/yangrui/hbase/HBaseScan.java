package com.yangrui.hbase;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

public class HBaseScan {

    public static void main(String[] args) throws IOException {
        //Scan类常用方法说明
        //指定需要的family或column ，如果没有调用任何addFamily或Column，会返回所有的columns；
        // scan.addFamily();
        // scan.addColumn();
        // scan.setMaxVersions(); //指定最大的版本个数。如果不带任何参数调用setMaxVersions，表示取所有的版本。如果不掉用setMaxVersions，只会取到最新的版本.
        // scan.setTimeRange(); //指定最大的时间戳和最小的时间戳，只有在此范围内的cell才能被获取.
        // scan.setTimeStamp(); //指定时间戳
        // scan.setFilter(); //指定Filter来过滤掉不需要的信息
        // scan.setStartRow(); //指定开始的行。如果不调用，则从表头开始；
        // scan.setStopRow(); //指定结束的行（不含此行）；
        // scan.setBatch(); //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。

        //过滤器
        //1、FilterList代表一个过滤器列表
        //FilterList.Operator.MUST_PASS_ALL -->and
        //FilterList.Operator.MUST_PASS_ONE -->or
        //eg、FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        //2、SingleColumnValueFilter
        //3、ColumnPrefixFilter用于指定列名前缀值相等
        //4、MultipleColumnPrefixFilter和ColumnPrefixFilter行为差不多，但可以指定多个前缀。
        //5、QualifierFilter是基于列名的过滤器。
        //6、RowFilter
        //7、RegexStringComparator是支持正则表达式的比较器。
        //8、SubstringComparator用于检测一个子串是否存在于值中，大小写不敏感。
//
//        HTable table=(HTable) getHTablePool().getTable("tb_stu");
//        Scan scan=new Scan();
//        scan.setMaxVersions();
//        //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
//        scan.setBatch(1000);
//
//        //scan.setTimeStamp(NumberUtils.toLong("1370336286283"));
//        //scan.setTimeRange(NumberUtils.toLong("1370336286283"), NumberUtils.toLong("1370336337163"));
//        //scan.setStartRow(Bytes.toBytes("quanzhou"));
//        //scan.setStopRow(Bytes.toBytes("xiamen"));
//        //scan.addFamily(Bytes.toBytes("info"));
//        //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"));
//
//        //查询列镞为info，列id值为1的记录
//        //方法一(单个查询)
//        // Filter filter = new SingleColumnValueFilter(
//        //         Bytes.toBytes("info"), Bytes.toBytes("id"), CompareOp.EQUAL, Bytes.toBytes("1"));
//        // scan.setFilter(filter);
//
//        //方法二(组合查询)
//        //FilterList filterList=new FilterList();
//        //Filter filter = new SingleColumnValueFilter(
//        //    Bytes.toBytes("info"), Bytes.toBytes("id"), CompareOp.EQUAL, Bytes.toBytes("1"));
//        //filterList.addFilter(filter);
//        //scan.setFilter(filterList);
//
//        ResultScanner rs = table.getScanner(scan);
//
//        for (Result r : rs) {
//            for (KeyValue kv : r.raw()) {
//                System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
//                        Bytes.toString(kv.getRow()),
//                        Bytes.toString(kv.getFamily()),
//                        Bytes.toString(kv.getQualifier()),
//                        Bytes.toString(kv.getValue()),
//                        kv.getTimestamp()));
//            }
//        }
//
//        rs.close();
    }
}
