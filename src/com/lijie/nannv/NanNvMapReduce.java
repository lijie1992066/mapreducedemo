package com.lijie.nannv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NanNvMapReduce extends Configured implements Tool {
	
	public static class NanNvMap extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(	LongWritable key, Text value,
							Mapper<LongWritable, Text, Text, Text>.Context context)	throws IOException,
																					InterruptedException {
			String[] split = value.toString().split("<tab>");
		
			context.write(new Text(split[2]), new Text(split[0]+"\t"+split[1]+"\t"+split[3]));
		}
	}
	
	public static class NanNvReduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(	Text key, Iterable<Text> values,
								Reducer<Text, Text, Text, Text>.Context context)	throws IOException,
																					InterruptedException {
			int maxScore = Integer.MIN_VALUE;
			String name = " ";
			String age = " ";
			String gender = " ";
			int score = 0;
			for (Text val : values) {
				String[] valTokens = val.toString().split("\\t");
				score = Integer.parseInt(valTokens[2]);
				if (score > maxScore) {
					name = valTokens[0];
					age = valTokens[1];
					gender = key.toString();
					maxScore = score;
				}
			}
			context.write(new Text(name), new Text("age- " + age + "\t"+ gender + "\tscore-" + maxScore));
		}
		
	}
	
	public static class NanNvPartitioner extends Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int num) {
			String[] split = value.toString().split("\t");
			
			int age = Integer.parseInt(split[1]);
			
			if(num == 0) return 0;
			if(age<20) return 0;
			if(age>=20&&age<50) return 1%num;
			if(age>=50) return 2%num;
			return 0;
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new Configuration(), new NanNvMapReduce(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();//读取配置文件

		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "gender");//新建一个任务
		job.setJarByClass(NanNvMapReduce.class);//主类
		job.setMapperClass(NanNvMap.class);//Mapper
		job.setReducerClass(NanNvReduce.class);//Reducer

		job.setPartitionerClass(NanNvPartitioner.class);//设置Partitioner类
		job.setNumReduceTasks(3);// reduce个数设置为3

		job.setMapOutputKeyClass(Text.class);//map 输出key类型
		job.setMapOutputValueClass(Text.class);//map 输出value类型

		job.setOutputKeyClass(Text.class);//输出结果 key类型
		job.setOutputValueClass(Text.class);//输出结果 value 类型

		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
		
		return job.waitForCompletion(true)?0:1;//提交任务
	}
	
}
