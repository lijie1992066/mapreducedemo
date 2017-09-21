package com.lijie.weibo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class WeiboInputFormat extends FileInputFormat<Text, WeiboPair> {
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public RecordReader<Text, WeiboPair> createRecordReader(InputSplit arg0,
															TaskAttemptContext arg1)	throws IOException,
																						InterruptedException {
		// TODO Auto-generated method stub
		return new WeiboRecordReader();
	}
	
}

class WeiboRecordReader extends RecordReader<Text, WeiboPair> {
	
	private Text lineKey;
	private WeiboPair lineValue;
	private LineReader in;
	private Text line;
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (in != null) {
			in.close();
		}
	}
	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lineKey;
	}
	
	@Override
	public WeiboPair getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lineValue;
	}
	
	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)	throws IOException,
																		InterruptedException {
		FileSplit fileSplit = (FileSplit) arg0;
		Configuration conf = arg1.getConfiguration();
		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(conf);
		
		FSDataInputStream open = fs.open(path);
		in = new LineReader(open, conf);
		
		lineKey = new Text();
		lineValue = new WeiboPair();
		line = new Text();
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		int readLine = in.readLine(line);
		
		if (readLine == 0) {
			return false;
		}
		
		String[] split = line.toString().split("\t");
		
		if (split.length != 5) {
			throw new IOException(" cuo wu shu ju ");
		}
		
		int a, b, c;
		
		a = Integer.parseInt(split[2]);
		b = Integer.parseInt(split[3]);
		c = Integer.parseInt(split[4]);
		
		lineKey.set(new Text(split[0]));
		lineValue.set(a, b, c);
		
		
		return true;
	}
	
}
