package com.lijie.joinreduce;

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

public class StationFileInputFormat extends FileInputFormat<Text, TextPair>{

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public RecordReader<Text, TextPair> createRecordReader(	InputSplit arg0,
															TaskAttemptContext arg1)	throws IOException,
																						InterruptedException {
		// TODO Auto-generated method stub
		return new StationRecordReader();
	}
	
}

class StationRecordReader extends RecordReader<Text, TextPair>{

	private Text lineKey;
	private TextPair lineValue;
	private LineReader in;
	private Text line;
	
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(in!= null) in.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lineKey;
	}

	@Override
	public TextPair getCurrentValue() throws IOException, InterruptedException {
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
		line = new Text();
		lineValue = new TextPair();
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		int readLine = in.readLine(line);
		if(readLine == 0){
			return false;
		}
		String[] split = line.toString().split("\\s+");
		if(split.length == 2){
			lineKey.set(split[1]);
			lineValue.set(new Text(split[0]), new Text("0"));
			return true;
		}else{
			throw new IOException("shujucuowu");
		}
	}}
