package hdfs;

import java.io.FileOutputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsSourceTrace {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.1.100:9000/");
		conf.set("dfs.replication", "1");
		System.setProperty("hadoop.home.dir", "C:\\softwareDir\\hadoop-2.4.1");
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.100:9000/"), conf, "root");
		FSDataInputStream is = fs.open(new Path("/jdk.tgz.rename"));
		FileOutputStream os = new FileOutputStream("F:/jdk.tgz.rename");
		IOUtils.copy(is, os);
	}
}
