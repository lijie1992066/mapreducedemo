package com.lijie.conter;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CounterMapReduce extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		String[] args1 = {"hdfs://lijie:9000/middle/*","hdfs://lijie:9000/middle/out"};
		int run = ToolRunner.run(new Configuration(), new CounterMapReduce(), args1);
		System.exit(run);
	}
	
	public static enum LOG_COUNTER{
		BAD_RECORDS;
	}
	
	public static class CounterMap extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(	LongWritable key, Text value,
							Mapper<LongWritable, Text, Text, Text>.Context context)	throws IOException,
																					InterruptedException {
			List<String> list = ParseTVData.transData(value.toString());
		
			if(list == null || list.size() == 0){
				Counter counter = context.getCounter(LOG_COUNTER.BAD_RECORDS);
				counter.increment(1);
				
				Counter counter2 = context.getCounter("log_counter", "bad_records");
				counter2.increment(1);
				
			}else{
				for (String string : list) {
					context.write(new Text(string), new Text(""));
				}
			}
			
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Path path = new Path(arg0[1]);
		FileSystem fs = path.getFileSystem(conf);
		if(fs.isDirectory(path)){
			fs.delete(path, true);
		}
		
		Job job = new Job(conf,"counter");
		job.setJarByClass(CounterMapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, path);
		
		job.setMapperClass(CounterMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//yasuo
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
}
