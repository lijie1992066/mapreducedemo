package com.lijie.joinreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StationDataMap extends Mapper<LongWritable, Text, TextPair, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)	throws IOException,
																		InterruptedException {
		String[] split = value.toString().split("\\s+", 2);
	
		context.write(new TextPair(new Text(split[0]), new Text("1")), new Text(split[1]));
	}
}
