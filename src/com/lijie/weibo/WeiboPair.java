package com.lijie.weibo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WeiboPair implements WritableComparable<WeiboPair> {
	
	private int fss;
	private int gzs;
	private int wbs;
	
	public void set(int fss, int gzs, int wbs) {
		this.fss = fss;
		this.gzs = gzs;
		this.wbs = wbs;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + fss;
		result = prime * result + gzs;
		result = prime * result + wbs;
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
		WeiboPair other = (WeiboPair) obj;
		if (fss != other.fss)
			return false;
		if (gzs != other.gzs)
			return false;
		if (wbs != other.wbs)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "WeiboPair [fss=" + fss + ", gzs=" + gzs + ", wbs=" + wbs + "]";
	}
	
	public int getFss() {
		return fss;
	}
	
	public void setFss(int fss) {
		this.fss = fss;
	}
	
	public int getGzs() {
		return gzs;
	}
	
	public void setGzs(int gzs) {
		this.gzs = gzs;
	}
	
	public int getWbs() {
		return wbs;
	}
	
	public void setWbs(int wbs) {
		this.wbs = wbs;
	}
	
	public WeiboPair() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public WeiboPair(int fss, int gzs, int wbs) {
		super();
		this.fss = fss;
		this.gzs = gzs;
		this.wbs = wbs;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		fss=in.readInt();
		gzs=in.readInt();
		wbs=in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(fss);
		out.writeInt(gzs);
		out.writeInt(wbs);
	}
	
	@Override
	public int compareTo(WeiboPair o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
