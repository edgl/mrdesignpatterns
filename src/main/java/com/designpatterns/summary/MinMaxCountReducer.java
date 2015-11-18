package com.designpatterns.summary;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

	private MinMaxCountTuple result = new MinMaxCountTuple();
	
	@Override
	protected void reduce(Text key, Iterable<MinMaxCountTuple> inValues,
			Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>.Context context)
					throws IOException, InterruptedException {
		
		// Iterate over the list
		for(MinMaxCountTuple tuple : inValues) {
			result.setCount(result.getCount() + tuple.getCount());
			
			if(result.getMin().compareTo(tuple.getMin()) > 0)
				result.setMin(tuple.getMin());
			
			if(result.getMax().compareTo(tuple.getMax()) < 0)
				result.setMax(tuple.getMax());
			
		}
		
		context.write(key, result);
	}
}
