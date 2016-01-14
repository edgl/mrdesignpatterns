package com.designpatterns.summary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SummaryDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new SummaryDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("Summary");
		job.setJarByClass(SummaryDriver.class);
		
		job.setMapperClass(MinMaxCountMapper.class);
		job.setReducerClass(MinMaxCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MinMaxCountTuple.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
