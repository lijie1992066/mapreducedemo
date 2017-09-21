package com.lijie.inutformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ScoreMapReduce extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		String[] path = {"hdfs://lijie:9000/score/*","hdfs://lijie:9000/score/out"};
		int run = ToolRunner.run(new Configuration(), new ScoreMapReduce(), path);
		System.exit(run);
	}
	
	public static class ScoreMap extends Mapper<Text, ScorePair, Text, ScorePair> {
		@Override
		protected void map(Text key, ScorePair value, Context context)	throws IOException,
																		InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class ScoreReduce extends Reducer<Text, ScorePair, Text, Text> {
		@Override
		protected void reduce(	Text key, Iterable<ScorePair> values,
								Context context) throws IOException, InterruptedException {
			ScorePair value = values.iterator().next();
			//sum
			float sum = value.getA()+value.getB()+value.getC()+value.getD()+value.getE();
			//avg
			float avg = sum/5;
			context.write(key, new Text("sum:"+sum+"\t"+"avg:"+avg));
		}
	}
	
	@Override
	public int run(String[] arg) throws Exception {
		Configuration conf = new Configuration();
		Path path = new Path(arg[1]);
		FileSystem fs = path.getFileSystem(conf);
		if(fs.isDirectory(path)){
			fs.delete(path, true);
		}
		
		Job job = new Job(conf, "score");
		
		job.setJarByClass(ScoreMapReduce.class);
		
		job.setMapperClass(ScoreMap.class);
		job.setReducerClass(ScoreReduce.class);
		
		job.setInputFormatClass(ScoreInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, path);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScorePair.class);
		
		job.waitForCompletion(true);
		return 0;
	}
	
}
