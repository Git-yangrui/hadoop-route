package com.yangrui.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

public class HbasTest {
	@Test
	public void testPut() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "yangziyu01:2181,yangziyu02:2181,yangziyu03:2181");
	    HTable mygirlsTable=new HTable(configuration, "mygirls");
//	    Put put =new Put(row)
	}
}
