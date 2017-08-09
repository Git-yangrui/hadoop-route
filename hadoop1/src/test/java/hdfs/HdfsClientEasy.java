package hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientEasy {

	private FileSystem fs = null;

	@Before
	public void getFs() throws IOException, Exception, URISyntaxException {

		// get a configuration object
		Configuration conf = new Configuration();
		// to set a parameter, figure out the filesystem is hdfs
		conf.set("fs.defaultFS", "hdfs://192.168.1.100:9000/");
		conf.set("dfs.replication", "1");
		System.setProperty("hadoop.home.dir", "C:\\softwareDir\\hadoop-2.4.1");
		fs = FileSystem.get(new URI("hdfs://192.168.1.100:9000/"), conf, "root");

	}
     @Test
     public void test_download() throws Exception, IOException{
    	 fs.copyToLocalFile(new Path("/jdk-7u80-linux-x64.tar.gz"), new Path("F:\\fromFS.tar"));
     }
	@Test
	public void testUpload() throws IllegalArgumentException, IOException {
		fs.copyFromLocalFile(new Path("E:\\新建文本文档.txt"), new Path("/src"));
	}

	@Test
	public void testRmfile() throws IllegalArgumentException, IOException {

		boolean res = fs.delete(new Path("/aa/bb/fromFS.tar"), true);

		System.out.println(res ? "delete is successfully :)" : "it is failed :(");

	}

	@Test
	public void testMkdir() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/aa/bb/cc"));
		System.out.println("-----------------");
	}

	@Test
	public void testRename() throws IllegalArgumentException, IOException {
		fs.rename(new Path("/aa/bb/fromFS.tar"), new Path("/jdk.tgz.rename"));
	}

	@Test
	public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus file = listFiles.next();
			System.out.println(file.getPath().getName());
		}
		System.out.println("--------------------------------------------");

		FileStatus[] status = fs.listStatus(new Path("/"));
		for (FileStatus file : status) {
			System.out.println(file.getPath().getName() + "   " + (file.isDirectory() ? "d" : "f"));
		}
	}

}
