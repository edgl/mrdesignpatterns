package com.designpatterns.summary;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>{

	private MinMaxCountTuple result = new MinMaxCountTuple();
	
	@Override
	protected void reduce(Text key, Iterable<MinMaxCountTuple> values,
			Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>.Context context) throws IOException, InterruptedException {

		result.setCount(0);
		result.setMax(null);
		result.setMin(null);
		
		long sum = 0;
		
		for(MinMaxCountTuple tuple : values) {
			sum++;
			if(result.getMax() == null || result.getMax().getTime() < tuple.getMax().getTime())
				result.setMax(tuple.getMax());
			
			if(result.getMin() == null || result.getMin().getTime() > tuple.getMin().getTime())
				result.setMin(tuple.getMin());
				
		}
		
		result.setCount(sum);
		
		context.write(key, result);
		
		
	}
}
