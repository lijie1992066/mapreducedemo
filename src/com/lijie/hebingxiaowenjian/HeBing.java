package com.lijie.hebingxiaowenjian;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

public class HeBing {

	
	public static void main(String[] args) throws IllegalArgumentException, Exception {
		uploadCom(new Path("F://73/*"), new Path("/middle"));
	}
	
	public static void uploadCom(Path src,Path dest) throws Exception{
		Configuration conf = new Configuration();
		URI uri = new URI("hdfs://lijie:9000");
		FileSystem fs = FileSystem.get(uri, conf);
		LocalFileSystem lfs = FileSystem.getLocal(conf);
		FileStatus[] gs1 = lfs.globStatus(src, new NoFilter("^.*svn$"));
		Path[] ps1 = FileUtil.stat2Paths(gs1);
		FSDataInputStream in = null;
		FSDataOutputStream out = null;
		for (Path path : ps1) {
			String name = path.getName().replaceAll("-", "");
			FileStatus[] gs2 = lfs.globStatus(new Path(path+"/*"), new YesFilter("^.*txt$"));
			Path[] ps2 = FileUtil.stat2Paths(gs2);
			Path destNow = new Path(dest+"/"+name+".txt");
			out = fs.create(destNow);
			for (Path path2 : ps2) {
				in = lfs.open(path2);
				IOUtils.copyBytes(in, out, 4096, false);
				in.close();
			}
			out.close();
		}
	}
	
}



class YesFilter implements PathFilter{

	private String reg;

	public YesFilter(String reg) {
		super();
		this.reg = reg;
	}

	@Override
	public boolean accept(Path arg0) {
		// TODO Auto-generated method stub
		return arg0.toString().matches(reg);
	}
}

class NoFilter implements PathFilter{

	private String reg;
	
	public NoFilter(String reg) {
		super();
		this.reg = reg;
	}

	@Override
	public boolean accept(Path arg0) {
		
		return !arg0.toString().matches(reg);
		
	}
}