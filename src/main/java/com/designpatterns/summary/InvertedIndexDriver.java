package com.designpatterns.summary;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utilities.MRDPUtils;

public class InvertedIndexDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new InvertedIndexDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("Inverted Index");
		job.setJarByClass(InvertedIndexDriver.class);
		
		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	private static class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text Id = new Text();
		private Text WikiHtml = new Text();
		private static final Pattern pattern = Pattern.compile(".*?<a\\s+href=\"(.*)\">.*");
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if(!value.toString().trim().contains("<row")) {
				return;
			}
			
			Map<String, String> attr = MRDPUtils.tranformXMLtoMap(value.toString().trim());
			
			if(attr != null && attr.size() > 0) {
				// get he post type
				String body = attr.get("Body");
				String wikihtml = getWikipediaURL(body, context);
				
				if(wikihtml == null)
					return;
				
				
				String id = attr.get("Id");
				
				Id.set(id);
				WikiHtml.set(wikihtml);
				
				context.write(WikiHtml, Id);
			}
		}
		

		private static String getWikipediaURL(String body, Context context) {
			context.getCounter("Job Counters", "method call hits").increment(1);
			if(body.contains("a href=&quot;https://www.wikipedia.org")) {
				
				String unescapedBody = StringEscapeUtils.unescapeHtml(body);
				Matcher matcher = pattern.matcher(unescapedBody);
				if(matcher.matches()) {
					
					String link = matcher.group(1);
					return link;
				}	
			}
			
			return null;
		}
	}
	
	private static class FilterReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text output = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			StringBuilder builder = new StringBuilder();
			
			int count = 0;
			while(values.iterator().hasNext()) {
				values.iterator().next();
				count ++;
			}
			
			if(count < 5)
				return;
			
			for(Text text : values) {
				builder.append(" ");
				builder.append(text.toString());
			}
			
			output.set(builder.toString());
			context.write(key, output);
			
		}
	}
}
