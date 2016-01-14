package com.designpatterns.summary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTuple implements Writable {

	private Date min = new Date();
	private Date max = new Date();
	private long count = 0;
	private static final SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSS");
	public Date getMin() {
		return min;
	}
	public void setMin(Date min) {
		this.min = min;
	}
	public Date getMax() {
		return max;
	}
	public void setMax(Date max) {
		this.max = max;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public void readFields(DataInput input) throws IOException {
		min = new Date(input.readLong());
		max = new Date(input.readLong());
		count = input.readLong();
		
	}
	public void write(DataOutput output) throws IOException {
		output.writeLong(min.getTime());
		output.writeLong(max.getTime());
		output.writeLong(count);
		
		
	}
	
	@Override
	public String toString() {
		return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
	}
}
