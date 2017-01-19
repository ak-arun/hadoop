package com.ak.mapreduce.orc.mapper;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ORCMapper extends Mapper<Object, Text, Text, NullWritable>{
	
	
	 public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
		 
		 context.write(value, NullWritable.get());
		 
	 }

}
