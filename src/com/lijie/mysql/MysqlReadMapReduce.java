package com.lijie.mysql;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MysqlReadMapReduce extends Configured implements Tool {
	
	public static class MysqlReadMap extends Mapper<LongWritable, TextPair, Text, Text> {
		@Override
		protected void map(	LongWritable key, TextPair value,
							Mapper<LongWritable, TextPair, Text, Text>.Context context)	throws IOException,
																						InterruptedException {
			
			context.write(new Text(value.getId() + ""),
				new Text(value.getEmail() + "\t" + value.getName()));
			
		}
	}
	
	public static class MysqlReadReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(	Text key, Iterable<Text> values,
								Reducer<Text, Text, Text, Text>.Context context)	throws IOException,
																					InterruptedException {
			
			Iterator<Text> iterator = values.iterator();
			
			while (iterator.hasNext()) {
				Text next = new Text(iterator.next());
				context.write(key, next);
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		String[] args1 = { "", "hdfs://lijie:9000/mysql/out" };
		int run = ToolRunner.run(new Configuration(), new MysqlReadMapReduce(), args1);
		
		System.exit(run);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = new Configuration();
		Path path = new Path(arg0[1]);
		FileSystem fs = path.getFileSystem(conf);
		
		if (fs.isDirectory(path)) {
			fs.delete(path, true);
		}
		org.apache.hadoop.filecache.DistributedCache.addFileToClassPath(
			new Path("hdfs://lijie:9000/mysql/jar/mysql-connector-java-5.1.14.jar"), conf);
		
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
			"jdbc:mysql://192.168.2.104:3306/hadooptest", "root", "");
		
		Job job = Job.getInstance();
		job.setJarByClass(MysqlReadMapReduce.class);
		job.setMapperClass(MysqlReadMap.class);
		job.setReducerClass(MysqlReadReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(DBInputFormat.class);
		FileOutputFormat.setOutputPath(job, path);
		
		String[] fields = { "id", "email", "name" };
		
		//1.Job;2.Class< extends DBWritable> 3.表名;4.where条件 5.order by语句;6.列名
		DBInputFormat.setInput(job, TextPair.class, "user", null, null, fields);
		
		job.waitForCompletion(true);
		return 0;
	}
	
}
