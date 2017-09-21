package com.lijie.uploadsingle;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UploadSingle {
	public static void main(String[] args) throws URISyntaxException, IOException {
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://lijie:9000");
		FileSystem fs = FileSystem.get(uri,conf);
		Path resP = new Path("F://a.txt");
		Path destP = new Path("/aaaaaa");
		if(!fs.exists(destP)){
			fs.mkdirs(destP);
		}
		String name = "F://a.txt".substring("F://a.txt".lastIndexOf("/")+1, "F://a.txt".length());
		fs.copyFromLocalFile(resP, destP);
		System.out.println("name is " + name);
		fs.close();
	}
}
