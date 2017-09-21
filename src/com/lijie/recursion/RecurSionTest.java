package com.lijie.recursion;

import java.io.File;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author lijie
 *
 */
public class RecurSionTest {
	public static void main(String[] args) throws Exception {
		
		//调用递归打印目录的方法
		printDir("/");
		
	}
	
/**
 * 递归打印本地目录
 * @param file
 */
public static void print(File file) {
	
	if (file.isDirectory()) {
		
		File[] fileArray = file.listFiles();
		
		if (fileArray != null) {
			
			for (int i = 0; i < fileArray.length; i++) {
				
				//递归调用
				print(fileArray[i]);
			}
		}
		
		System.out.println(file);
	}
}
	
	/**
	 * 递归打印目录
	 * @param dir
	 * @throws Exception
	 */
	public static void printDir(String dir) throws Exception {
		
		FileSystem fs = getFileSystem();
		
		FileStatus[] globStatus = fs.listStatus(new Path(dir));
		
		Path[] stat2Paths = FileUtil.stat2Paths(globStatus);
		
		for (int i = 0; i < stat2Paths.length; i++) {
			
			System.out.println(stat2Paths[i].toString());
			
			//判断是否为文件夹
			if (fs.isDirectory(stat2Paths[i])) {
				
				//递归调用
				printDir(stat2Paths[i].toString());
			}
		}
	}
	
	/**
	 * 获取FileSystem
	 * @param dir
	 * @return
	 * @throws Exception
	 */
	public static FileSystem getFileSystem() throws Exception {
		
		Configuration conf = new Configuration();
		
		URI uri = new URI("hdfs://lijie:9000");
		
		FileSystem fs = FileSystem.get(uri, conf);
		
		return fs;
	}
}
