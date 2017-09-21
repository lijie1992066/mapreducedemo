package com.lijie.temperature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Temperature extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		String[] a = {"hdfs://lijie:9000/weather/","hdfs://lijie:9000/weather/out"};
		int run = ToolRunner.run(new Configuration(), new Temperature(), a);
		System.exit(run);
	}
	
	public static class TemperatureMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		protected void map(	LongWritable key, Text value,
							Context context) throws java.io.IOException, InterruptedException {
			String line = value.toString();
			int temperature = Integer.parseInt(line.substring(14, 19).trim());
			
			if (temperature != -9999) {
				FileSplit inputSplit = (FileSplit) context.getInputSplit();
				String substring = inputSplit.getPath().getName().substring(5, 10);
				
				context.write(new Text(substring), new IntWritable(temperature));
				
			}
		};
	}
	
	public static class TemperatureReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		@Override
		protected void reduce(	Text key, Iterable<IntWritable> values,
								Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (IntWritable intWritable : values) {
				sum += intWritable.get();
				count++;
			}
			int avg = sum / count;
			result.set(avg);
			context.write(key, result);
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Path myPath = new Path(arg0[1]);
		FileSystem fileSystem = myPath.getFileSystem(conf);
		if (fileSystem.isDirectory(myPath)) {
			fileSystem.delete(myPath, true);
		}
		Job job = new Job(conf, "temperature");
		job.setJarByClass(Temperature.class);
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, myPath);
		
		job.setMapperClass(TemperatureMap.class);
		job.setReducerClass(TemperatureReduce.class);
		
		//reduce 输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		return 0;
	}
}
