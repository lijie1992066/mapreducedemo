package com.lijie.joinreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	private Text first;
	private Text second;
	
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TextPair other = (TextPair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}
	
	public TextPair() {
		set(new Text(), new Text());
	}
	
	public TextPair(Text first, Text second) {
		super();
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst() {
		return first;
	}
	
	public void setFirst(Text first) {
		this.first = first;
	}
	
	public Text getSecond() {
		return second;
	}
	
	public void setSecond(Text second) {
		this.second = second;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(arg0);
		second.readFields(arg0);
	}
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		first.write(arg0);
		second.write(arg0);
	}
	
	@Override
	public int compareTo(TextPair o) {
		
		if (!first.toString().equals(o.first.toString()))
			return first.toString().compareTo(o.first.toString());
		if (!second.toString().equals(o.second.toString()))
			return second.toString().compareTo(o.second.toString());
		return 0;
	}
	
}
