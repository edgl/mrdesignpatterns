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
	
	public final static SimpleDateFormat frmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
	
	
	public void readFields(DataInput in) throws IOException {
		// Read the data out in the order it is written,
		// creating the new date objects from the UNIX timestamp
		min = new Date(in.readLong());
		max = new Date(in.readLong());
		count = in.readLong();

	}

	public void write(DataOutput out) throws IOException {
		// write the data out in the order it is read,
		// using the UNIX timestamp to represent the date
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		out.writeLong(count);

	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
	}

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

}
