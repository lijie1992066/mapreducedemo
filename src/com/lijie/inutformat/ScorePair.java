package com.lijie.inutformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ScorePair implements WritableComparable<ScorePair>{

	private float a;
	private float b;
	private float c;
	private float d;
	private float e;
	
	
	public float getA() {
		return a;
	}

	public void setA(float a) {
		this.a = a;
	}

	public float getB() {
		return b;
	}

	public void setB(float b) {
		this.b = b;
	}

	public float getC() {
		return c;
	}

	public void setC(float c) {
		this.c = c;
	}

	public float getD() {
		return d;
	}

	public void setD(float d) {
		this.d = d;
	}

	public float getE() {
		return e;
	}

	public void setE(float e) {
		this.e = e;
	}

	public ScorePair() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ScorePair(float a, float b, float c, float d, float e) {
		super();
		this.a = a;
		this.b = b;
		this.c = c;
		this.d = d;
		this.e = e;
	}
	public void set(float a, float b, float c, float d, float e) {
		this.a = a;
		this.b = b;
		this.c = c;
		this.d = d;
		this.e = e;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(a);
		result = prime * result + Float.floatToIntBits(b);
		result = prime * result + Float.floatToIntBits(c);
		result = prime * result + Float.floatToIntBits(d);
		result = prime * result + Float.floatToIntBits(e);
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
		ScorePair other = (ScorePair) obj;
		if (Float.floatToIntBits(a) != Float.floatToIntBits(other.a))
			return false;
		if (Float.floatToIntBits(b) != Float.floatToIntBits(other.b))
			return false;
		if (Float.floatToIntBits(c) != Float.floatToIntBits(other.c))
			return false;
		if (Float.floatToIntBits(d) != Float.floatToIntBits(other.d))
			return false;
		if (Float.floatToIntBits(e) != Float.floatToIntBits(other.e))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		a=in.readFloat();
		b=in.readFloat();
		c=in.readFloat();
		d=in.readFloat();
		e=in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeFloat(a);
		out.writeFloat(b);
		out.writeFloat(c);
		out.writeFloat(d);
		out.writeFloat(e);
	}

	@Override
	public int compareTo(ScorePair o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
