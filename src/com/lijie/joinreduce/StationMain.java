package com.lijie.joinreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StationMain extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		String[] args1 = {"hdfs://lijie:9000/join/station1.txt","hdfs://lijie:9000/join/station2.txt","hdfs://lijie:9000/join/out"};
		System.exit(ToolRunner.run(new Configuration(), new StationMain(), args1));
	}
	
	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = new Configuration();
		
		Path path = new Path(arg0[2]);
		
		FileSystem fs = path.getFileSystem(conf);
		
		if(fs.isDirectory(path)){
			fs.delete(path, true);
		}
		
		Job job = new Job(conf, "stationjoin");
		job.setJarByClass(StationMain.class);
		
		MultipleInputs.addInputPath(job, new Path(arg0[0]), StationFileInputFormat.class, StationMapper.class);
		MultipleInputs.addInputPath(job, new Path(arg0[1]), TextInputFormat.class, StationDataMap.class);
		
		FileOutputFormat.setOutputPath(job, path);
		
		job.setReducerClass(StationReduce.class);
		
		job.setNumReduceTasks(2);
		
		job.setPartitionerClass(StationPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
		return 0;
	}
	
}
