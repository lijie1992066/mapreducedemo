package com.lijie.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class TemperatureTest {
	
	private Mapper<LongWritable, Text, Text, IntWritable> mapper;
	
	private Reducer<Text, IntWritable, Text, IntWritable> reduce;
	
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	@Before
	public void init(){
		
		mapper = new Temperature.TemperatureMap();
		
		reduce = new Temperature.TemperatureReduce();
	}
	
	@Test
	public void test(){
		String line = "1980 12 01 00    78   -17 10237   180    21     1     0     0";
		mapDriver.withInput(new LongWritable(), new Text(line)).withOutput(new Text("11114"), new IntWritable(78)).runTest();
		
	}
}
