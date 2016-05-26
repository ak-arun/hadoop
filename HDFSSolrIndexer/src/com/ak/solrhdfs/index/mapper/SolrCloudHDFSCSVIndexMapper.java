package com.ak.solrhdfs.index.mapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.mortbay.log.Log;

import com.ak.solrhdfs.index.utils.IndexHelper;

public class SolrCloudHDFSCSVIndexMapper extends
		Mapper<LongWritable, Text, NullWritable, NullWritable> {

	
	private CloudSolrClient client;
	private String[] headers;
	private int numColumns;
	private IndexHelper helper;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		helper = IndexHelper.getInstance();
		super.setup(context);
		Configuration config = context.getConfiguration();
		client = new CloudSolrClient(config.get("solr.zk.string"));
		client.setDefaultCollection(config.get("solr.default.collection"));
		headers = helper.getArrayFromCsvString(config.get("solr.input.csv.header"));
		numColumns = headers.length;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		try {
			client.commit();
		} catch (SolrServerException e) {
			Log.info("Document commit error", e);
			e.printStackTrace();
		}
		client.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String valueString = value.toString();
		

		String[] valueArray = helper.getArrayFromCsvString(valueString);
		if (valueArray.equals(headers)) {
			Log.info("Skipping Header "+valueString);
		} else {
			if (valueArray.length != numColumns) {
				Log.info("Column Count does not match for line "
						+ valueString + " skipping row");
			} else {

				SolrInputDocument document = new SolrInputDocument();
				for (int index = 0; index < numColumns; index++) {
					if (valueArray[index] != null) {
						
						Log.info("Adding "+helper.getHeaderForIndex(index,headers)+"-"+valueArray[index]);
						
						document.addField(helper.getHeaderForIndex(index,headers),
								valueArray[index]);
					}else{
						Log.info("Nothing to index");
					}
				}

				try {
					client.add(document);
					context.write(NullWritable.get(), NullWritable.get());
				} catch (SolrServerException e) {
					Log.info("Document addition error",e);
					e.printStackTrace();
				}

			}
		}

	}

}
