package com.ak.solrhdfs.index.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.ak.solrhdfs.index.mapper.SolrCloudHDFSCSVIndexMapper;

public class SampleCSVIndexer {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("solr.zk.string","localhost:9983");
		conf.set("solr.default.collection","c2");
		conf.set("solr.input.csv.header","name,age,address");
		conf.set("mapreduce.job.user.classpath.first", "true");
		
		
		Job job = Job.getInstance(conf, "SampleCSVIndexer");
		job.setJarByClass(SampleCSVIndexer.class);
		job.addArchiveToClassPath(new Path("/ak/indexlibs"));
		job.setMapperClass(SolrCloudHDFSCSVIndexMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/ak/csv/"));
		job.waitForCompletion(true);
	}

}
