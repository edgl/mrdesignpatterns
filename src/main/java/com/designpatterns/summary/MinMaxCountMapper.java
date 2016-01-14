package com.designpatterns.summary;

import static utilities.MRDPUtils.tranformXMLtoMap;

import java.awt.datatransfer.StringSelection;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple>{

	private Text outUserId = new Text();
	private MinMaxCountTuple tuple = new MinMaxCountTuple();
	private static final SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSS");
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, MinMaxCountTuple>.Context context)
			throws IOException, InterruptedException {
		
		if(!value.toString().contains("<row"))
			return;

		Map<String, String> parsed = tranformXMLtoMap(value.toString());
		String strDate = parsed.get("CreationDate");
		
		// UserId
		String userid = parsed.get("AccountId");
		if(userid == null || userid.length() == 0) {
			return;
		}
		
		outUserId.set(userid);
		
		Date creationDate;
		try {
			creationDate = frmt.parse(strDate);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
		
		tuple.setMax(creationDate);
		tuple.setMin(creationDate);
		tuple.setCount(1);
		
		context.write(outUserId, tuple);
	}
}
