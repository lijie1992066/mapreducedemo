package com.lijie.inutformat;

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


public class ScoreInputFormat extends FileInputFormat<Text, ScorePair>{

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public RecordReader<Text, ScorePair> createRecordReader(	InputSplit arg0,
															TaskAttemptContext arg1)	throws IOException,
																						InterruptedException {
		// TODO Auto-generated method stub
		return new ScoreRecordReader();
	}
	
}

class ScoreRecordReader extends RecordReader<Text, ScorePair>{

	private LineReader in;
	private Text lineKey;
	private ScorePair lineValue;
	private Text line;
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(in !=null){
			in.close();
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lineKey;
	}

	@Override
	public ScorePair getCurrentValue() throws IOException, InterruptedException {
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
		FileSplit split = (FileSplit)arg0;
		Configuration conf = arg1.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		
		FSDataInputStream fileIn = fs.open(path);
		in = new LineReader(fileIn,conf);
		line = new Text();
		lineKey = new Text();
		lineValue = new ScorePair();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		int lineSize = in.readLine(line);
		if(lineSize == 0) return false;
		String[] split = line.toString().split("\\s+");
		if(split.length != 7){
			throw new IOException("数据错误!");
		}
		float a,b,c,d,e;
		a = Float.parseFloat(split[2].trim());
		b = Float.parseFloat(split[3].trim());
		c = Float.parseFloat(split[4].trim());
		d = Float.parseFloat(split[5].trim());
		e = Float.parseFloat(split[6].trim());
		lineKey.set(split[0]+"\t"+split[1]);
		lineValue.set(a, b, c, d, e);
		return true;
	}
	
}
