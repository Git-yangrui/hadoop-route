package com.yangrui.hadoop.clustermode;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsClient {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		System.setProperty("HADOOP_USER_NAME", "root"); 
		
		/**
		 * need add core-sites.xml and hdfs-site.xml for set ns1 name service 
		 */
		FileSystem fs = FileSystem.get(new URI("hdfs://ns1/"), conf, "root");
		fs.copyFromLocalFile(new Path("E:\\eclipse-java-luna-SR2-linux-gtk-x86_64.tar.gz"), new Path("/"));
	}
}
