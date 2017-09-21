package com.lijie.joinreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StationMapper extends Mapper<Text, TextPair,TextPair,Text >{
	@Override
	protected void map(	Text key, TextPair value,
						Mapper<Text, TextPair, TextPair, Text>.Context context)	throws IOException,
																				InterruptedException {
		context.write(value, key);
	}
}
