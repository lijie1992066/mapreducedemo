package com.lijie.lixianxiangmu.pre;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeblogPreProcess {
	static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		Set<String> pages = new HashSet();
		Text k = new Text();
		NullWritable v = NullWritable.get();
		
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)	throws IOException,
																								InterruptedException {
			this.pages.add("/about");
			this.pages.add("/black-ip-list/");
			this.pages.add("/cassandra-clustor/");
			this.pages.add("/finance-rhive-repurchase/");
			this.pages.add("/hadoop-family-roadmap/");
			this.pages.add("/hadoop-hive-intro/");
			this.pages.add("/hadoop-zookeeper-intro/");
			this.pages.add("/hadoop-mahout-roadmap/");
		}
		
		protected void map(	LongWritable key, Text value,
							Mapper<LongWritable, Text, Text, NullWritable>.Context context)	throws IOException,
																							InterruptedException {
			String line = value.toString();
			WebLogBean webLogBean = WebLogParser.parser(line);
			
			WebLogParser.filtStaticResource(webLogBean, this.pages);
			
			this.k.set(webLogBean.toString());
			context.write(this.k, this.v);
		}
	}
	
	public static void main(String[] args) throws Exception {
		String[] arg = {"hdfs://192.168.80.123:9000/sz/access.log.fensi","hdfs://192.168.80.123:9000/sz/out1"};
		Configuration conf = new Configuration();
		Path path = new Path(arg[1]);
		FileSystem fs = path.getFileSystem(conf);
		if(fs.isDirectory(path)){
			fs.delete(path, true);
		}
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WeblogPreProcess.class);
		
		job.setMapperClass(WeblogPreProcessMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job,path);
		
		job.setNumReduceTasks(0);
		
		job.waitForCompletion(true);
	}
}
