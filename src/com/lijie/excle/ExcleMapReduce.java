package com.lijie.excle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExcleMapReduce extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {	"hdfs://lijie:9000/excel/phone.xls",
							"hdfs://lijie:9000/excel/out/" };
		int ec = ToolRunner.run(new Configuration(), new ExcleMapReduce(), args0);
		System.exit(ec);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 配置文件对象
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		
		Job job = new Job(conf);
		job.setJarByClass(ExcleMapReduce.class);
		job.setJobName("Excel Record Reader");
		job.setMapperClass(ExcleMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(ExcleInputFormat.class);//自定义输入格式
		
		job.setReducerClass(ExcleReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class ExcleMap extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)	throws IOException,
																			InterruptedException {
			String s = value.toString();
			String[] split = s.split("\\s+");
			
			String k = split[1];
			String v = split[2];
			
			context.write(new Text(k), new Text(v));
		}
	}
	
	public static class ExcleReduce extends Reducer<Text, Text, Text, Text> {
		
		
		@Override
		protected void reduce(	Text key, Iterable<Text> values,
								Context context) throws IOException, InterruptedException {
			int c = 0;
			
			Text next = values.iterator().next();
			for (Text text : values) {
				c++;
			}
			context.write(key, new Text(next + "\t" + c));
		}
	}
	
}
