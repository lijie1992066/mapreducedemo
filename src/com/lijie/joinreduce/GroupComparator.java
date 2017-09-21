package com.lijie.joinreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator{

	public GroupComparator() {
		super(TextPair.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		TextPair t1 = (TextPair) a;
		TextPair t2 = (TextPair) b;
		return t1.getFirst().compareTo(t2.getFirst());
	}
}
