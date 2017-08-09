package com.yangrui.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseDao {
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "yangziyu01:2181,yangziyu02:2181,yangziyu03:2181");
		@SuppressWarnings("deprecation")
		HBaseAdmin admin = new HBaseAdmin(configuration);
		TableName tableName = TableName.valueOf("mygirls");
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		HColumnDescriptor base_info=new HColumnDescriptor("base_info");
		base_info.setMaxVersions(3);
		
		hTableDescriptor.addFamily(base_info);
		admin.createTable(hTableDescriptor);
	}
}
