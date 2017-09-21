package com.lijie.multiplefileupload;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class MultipleFile {

	public static void main(String[] args) throws IllegalArgumentException, URISyntaxException, IOException {
		Path dstPath = new Path("/mutiple");
		uploadMultipleFile(new Path("F://test/*"), dstPath);
	}
	
	public static void uploadMultipleFile(Path srcPath,Path destPath) throws URISyntaxException, IOException{
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://lijie:9000");
		FileSystem fs = FileSystem.get(uri,conf);
		LocalFileSystem lfs = FileSystem.getLocal(conf);
		FileStatus[] globStatus = lfs.globStatus(srcPath, new MyPathFilter("^.*txt$"));
		Path[] paths = FileUtil.stat2Paths(globStatus);
		for (Path path : paths) {
			fs.copyFromLocalFile(path, destPath);
		}
		fs.close();
	}
}

class MyPathFilter implements PathFilter{

	private String reg;
	
	public MyPathFilter(String reg) {
		this.reg = reg;
	}
	
	@Override
	public boolean accept(Path path) {
		boolean flag = path.toString().matches(reg);
		return flag;
	}
	
}
