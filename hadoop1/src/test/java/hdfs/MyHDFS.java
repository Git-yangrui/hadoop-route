package hdfs;

import java.io.FileInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
	
public class MyHDFS {
    public static void main(String[] args) throws Exception {
    	
		Configuration configuration=new Configuration();
		configuration.set("fs.defaultFS", "hdfs://192.168.1.100:9000/");
		FileSystem fSystem=FileSystem.get(configuration);
		Path path = new Path("hdfs://192.168.1.100:9000/wc");
		FSDataOutputStream create = fSystem.create(path);
		FileInputStream fileInputStream = new FileInputStream("localfile");
		IOUtils.copy(fileInputStream, create);
		
	}	
}
