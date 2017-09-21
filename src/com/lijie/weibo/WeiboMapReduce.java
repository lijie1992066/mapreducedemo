package com.lijie.weibo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeiboMapReduce extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		String[] args0 = {	"hdfs://lijie:9000/weibo/weibo.txt",
							"hdfs://lijie:9000/weibo/weibo-out/" };
		int ec = ToolRunner.run(new Configuration(), new WeiboMapReduce(), args0);
		System.exit(ec);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = new Configuration();
		Path path = new Path(arg0[1]);
		FileSystem fs = path.getFileSystem(conf);
		
		if (fs.isDirectory(path)) {
			fs.delete(path, true);
		}
		
		Job job = new Job(conf, "weibo");
		job.setJarByClass(WeiboMapReduce.class);
		
		job.setMapperClass(WeiboMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(WeiboReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(WeiboInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, path);
		
		MultipleOutputs.addNamedOutput(job, "fensishu", TextOutputFormat.class, Text.class,
			IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "guanzhushu", TextOutputFormat.class, Text.class,
			IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "weiboshu", TextOutputFormat.class, Text.class,
			IntWritable.class);
		
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class WeiboMap extends Mapper<Text, WeiboPair, Text, Text> {
		@Override
		protected void map(	Text key, WeiboPair value,
							Mapper<Text, WeiboPair, Text, Text>.Context context)	throws IOException,
																					InterruptedException {
			context.write(new Text("fss"), new Text(key.toString() + "\t" + value.getFss()));
			context.write(new Text("gzs"), new Text(key.toString() + "\t" + value.getGzs()));
			context.write(new Text("wbs"), new Text(key.toString() + "\t" + value.getWbs()));
		}
	}
	
	public static class WeiboReduce extends Reducer<Text, Text, Text, IntWritable> {
		
		private MultipleOutputs<Text, IntWritable> mos;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, IntWritable>.Context context)	throws IOException,
																						InterruptedException {
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, IntWritable>.Context context)	throws IOException,
																						InterruptedException {
			mos.close();
		}
		
		@Override
		protected void reduce(	Text key, Iterable<Text> values,
								Reducer<Text, Text, Text, IntWritable>.Context context)	throws IOException,
																						InterruptedException {
			
			int N = context.getConfiguration().getInt("reduceHasMaxLength", Integer.MAX_VALUE);
			Map<String, Integer> map = new HashMap<>();
			
			for (Text text : values) {
				String[] split = text.toString().split("\t");
				
				map.put(split[0], Integer.parseInt(split[1]));
			}
			Entry[] entries = getSortedHashtableByValue(map);
			
			for (int i = 0; i < N && i < entries.length; i++) {
				if (key.toString().equals("fss"))
					mos.write("fensishu", entries[i].getKey(), entries[i].getValue());
				if (key.toString().equals("gzs"))
					mos.write("guanzhushu", entries[i].getKey(), entries[i].getValue());
				if (key.toString().equals("wbs"))
					mos.write("weiboshu", entries[i].getKey(), entries[i].getValue());
			}
			
		}
	}
	
	//对Map内的数据进行排序（只适合小数据量）
	public static Map.Entry[] getSortedHashtableByValue(Map h) {
		Set set = h.entrySet();
		Map.Entry[] entries = (Map.Entry[]) set.toArray(new Map.Entry[set.size()]);
		Arrays.sort(entries, new Comparator() {
			public int compare(Object arg0, Object arg1) {
				Long key1 = Long.valueOf(((Map.Entry) arg0).getValue().toString());
				Long key2 = Long.valueOf(((Map.Entry) arg1).getValue().toString());
				return key2.compareTo(key1);
			}
		});
		return entries;
	}
	
}
