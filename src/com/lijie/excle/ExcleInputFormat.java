package com.lijie.excle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ExcleInputFormat extends FileInputFormat<LongWritable, Text> {
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return super.isSplitable(context, filename);
	}
	
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(	InputSplit arg0,
																TaskAttemptContext arg1)	throws IOException,
																							InterruptedException {
		// TODO Auto-generated method stub
		return new ExcleRecordReader();
	}
	
}

class ExcleRecordReader extends RecordReader<LongWritable, Text> {
	
	private Text lineValue;
	private LongWritable lineKey;
	private FSDataInputStream in;
	private String[] lines;
	private int c = 0;
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (in != null) {
			in.close();
		}
	}
	
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lineKey;
	}
	
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
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
		
		FileSplit split = (FileSplit) arg0;
		
		Configuration conf = arg1.getConfiguration();
		
		Path path = split.getPath();
		
		FileSystem fs = path.getFileSystem(conf);
		
		in = fs.open(path);
		
		String line = new ExcleUtil().parseExcleData(in);
		
		lines = line.split("\n");
		
		lineKey = new LongWritable(0);
		
		lineValue = new Text();
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (lines.length == 0 || lines == null) {
			return false;
		}
		
		if (lines.length-1 > c) {
			String value = lines[c];
			lineKey = new LongWritable(c);
			lineValue = new Text(value);
			c++;
			return true;
		} else {
			
			return false;
		}
		
	}
	
}