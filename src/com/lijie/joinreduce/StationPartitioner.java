package com.lijie.joinreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StationPartitioner extends Partitioner<TextPair, Text>{

	@Override
	public int getPartition(TextPair key, Text value, int num) {
		// TODO Auto-generated method stub
		
		if(num == 0 ){
			return 0;
		}
		int a = (key.getFirst().hashCode()&Integer.MAX_VALUE)%num;
		return a;
	}
	
}
