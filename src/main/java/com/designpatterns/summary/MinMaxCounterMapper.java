package com.designpatterns.summary;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class MinMaxCounterMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {

	private final static SimpleDateFormat frmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
	
	private static Log logger = LogFactory.getLog(Mapper.class);
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, MinMaxCountTuple>.Context context)
			throws IOException, InterruptedException {
		
		Map<String, MinMaxCountTuple> parsed = new HashMap<String, MinMaxCountTuple>();
		try {
			logger.info(value.toString());
			//parsed = getTupleFromJson(value.toString());
		} catch (Exception e) {
			throw new RuntimeException(value.toString());
		}
		
		for(Entry<String, MinMaxCountTuple> entry : parsed.entrySet()) {
			context.getCounter("Custome", "numberofstatus").increment(1);
			
			System.out.println(entry.getKey().toString() + " " + entry.getValue().toString());
			context.write(new Text(entry.getKey()), entry.getValue());
		}
		
	}

	private Map<String, MinMaxCountTuple> getTupleFromJson(String json) throws Exception {
		Map<String, MinMaxCountTuple> map = new HashMap<String, MinMaxCountTuple>();
		
		JsonParser parser = new JsonParser();
		JsonElement el = parser.parse(json);
		
		JsonObject obj = el.getAsJsonObject();
		JsonArray arr = obj.get("statuses").getAsJsonArray();
		
		Iterator<JsonElement> elems = arr.iterator();
		while(elems.hasNext()) {
			
			
			JsonObject jObj =  elems.next().getAsJsonObject();
			String created_at = jObj.get("created_at").getAsString();
			Date dt_created_at = frmt.parse(created_at);
			String userId = jObj.get("user").getAsJsonObject().get("id").getAsString();
			
			MinMaxCountTuple tuple;
			if(map.containsKey(userId))
				map.get(userId).setCount(map.get(userId).getCount() + 1);
			else {
				tuple = new MinMaxCountTuple();
				tuple.setMax(dt_created_at);
				tuple.setMin(dt_created_at);
				tuple.setCount(1);
				map.put(userId, tuple);
			}
			
		}
			
		return map;
			
	}

}
