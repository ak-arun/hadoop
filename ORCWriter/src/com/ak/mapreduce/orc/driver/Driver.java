package com.ak.mapreduce.orc.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.BloomAllColumnOrcOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ak.mapreduce.orc.mapper.ORCMapper;
import com.ak.mapreduce.orc.reducer.ORCReducer;

public class Driver {
public static void main(String[] args) throws Exception{

	Configuration conf = new Configuration();
	conf.set("orc.compress", "ZLIB");
	conf.set("orc.mapred.output.schema","struct<key:string,age:int,name:string>");
	conf.set("orc.bloom.filter.columns", "*");
	conf.set("hello", "world");
	Job job = Job.getInstance(conf, "hello orc");
	job.setJarByClass(Driver.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(BloomAllColumnOrcOutputFormat.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(NullWritable.class);
	job.setMapperClass(ORCMapper.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(ORCReducer.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Writable.class);
	FileInputFormat.addInputPath(job, new Path("in"));
	FileOutputFormat.setOutputPath(job, new Path("out"+System.currentTimeMillis()));

  boolean completion= job.waitForCompletion(true) ;
  System.out.println(completion);
}
}
