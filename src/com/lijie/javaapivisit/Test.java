package com.lijie.javaapivisit;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class Test {
	
	public static void main(String[] args) throws Exception {
		mkdir();
//		delete();
//		copyFromHdfs();
//		showList();
//		getFileLocal();
//		hdfsInfo();
	}
	
	
	public static FileSystem getFileSystem() throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://lijie:9000");
		FileSystem fs = FileSystem.get(uri, conf);
//		FileSystem fs = FileSystem.get(conf);
		return fs;
	}
	
	public static void mkdir() throws Exception{
		FileSystem fs = getFileSystem();
		
		fs.mkdirs(new Path("/asdasdasd"));
		
		fs.close();
	}
	
	public static void delete() throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		fs.delete(new Path("/datatest"), true);
		fs.close();
	}
	
	public static void copyToHdfs() throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		Path path = new Path("F:\\aa.txt");
		Path path1 = new Path("/");
		fs.copyFromLocalFile(path, path1);
		fs.close();
	}
	
	public static void copyFromHdfs() throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		Path path1 = new Path("/dajiangtai/lijie.txt");
		Path paht2 = new Path("D://");
		fs.copyToLocalFile(path1, paht2);
		fs.close();
	}
	
	public static void showList() throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		FileStatus[] ls = fs.listStatus(new Path("/"));
		
		Path[] ps = FileUtil.stat2Paths(ls);
		for (Path path : ps) {
			System.out.println(path);
		}
		fs.close();
	}
	
	public static void getFileLocal() throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		Path path = new Path("/dajiangtai/lijie.txt");
		FileStatus fis = fs.getFileStatus(path);
		BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fis, 0, fis.getLen());
		int i=0;
		for (BlockLocation blockLocation : fileBlockLocations) {
			i++;
			System.out.println(blockLocation.getHosts()[i-1]+"----"+i);
		}
		fs.close();
	}
	
	public static void hdfsInfo() throws IOException, URISyntaxException{
		FileSystem fs = getFileSystem();
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		for (DatanodeInfo datanodeInfo : dataNodeStats) {
			System.out.println(datanodeInfo.getHostName());
		}
		fs.close();
	}
}
