package com.leoliu1221.hadoop.ibm;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class AverageFour {
 
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
    		
    	
    	
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
 
        // Create configuration
        Configuration conf = new Configuration(true);
 
        // Create job
        Job job = Job.getInstance(conf);
        //Job job = new Job(conf, "MaxTemperature");
        job.setJarByClass(AverageFour.class);
 
        // Setup MapReduce
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);
 
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
 
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
 
        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
 
    }
    public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

//    	private final IntWritable ONE = new IntWritable(1);
    	private Text word = new Text();

    	public void map(Object key, Text value, Context context)
    			throws IOException, InterruptedException {
    		//check if the last column is false. 
    		String[] data = value.toString().split(",");
    		if(!data[data.length-1].toLowerCase().equals("false")) return;
    		
    		
    		DoubleWritable four = new DoubleWritable();
    		
    		//combine the 30 31 32 33 into a unique id where if they dont have the same combination they wont be the same. 
    		String id = data[29]+','+data[30]+','+data[31]+','+data[32]+',';

    		
    		word.set(id);
    		four.set(Double.parseDouble(data[3]));
    		context.write(word, four);

    	}
    }
    
    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    	public void reduce(Text text, Iterable<DoubleWritable> values, Context context)
    			throws IOException, InterruptedException {
    		double sum = 0.0;
    		double count = 0.0;
    		for (DoubleWritable value : values) {
    			sum += value.get();
    			count += 1.0;
    		}
    		context.write(text, new DoubleWritable(sum / count));
    	}
    }
 
}