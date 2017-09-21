package com.lijie.joinreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StationReduce extends Reducer<TextPair, Text, Text, Text>{
	@Override
	protected void reduce(	TextPair key, Iterable<Text> value,
							Reducer<TextPair, Text, Text, Text>.Context context)	throws IOException,
																				InterruptedException {
		Iterator<Text> iterator = value.iterator();
		Text nextBH = new Text(iterator.next());
		Text stationName = key.getFirst();
		
		while(iterator.hasNext()){
			Text nextSJ = iterator.next();
			context.write(new Text(stationName), new Text(nextBH+"\t"+nextSJ));
		}
		
	}
}
