package com.ak.solrhdfs.index.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.mortbay.log.Log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;

public class IndexHelper {
	
	private Map<Integer,String>headerIndexMap;
	private CsvMapper mapper;
	private boolean headerMapped;
	
	private IndexHelper(){
		super();
	headerIndexMap=new HashMap<Integer, String>();	
	mapper = new CsvMapper();
	mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
	}
	
	private static IndexHelper instance;
	
	public static IndexHelper getInstance(){
		
		if(instance==null){
			instance = new IndexHelper();
		}
		return instance;
	}
	
	public String[] getArrayFromCsvString(String csv)
			throws JsonProcessingException, IOException {

		return (String[]) mapper.readerFor(String[].class).readValues(csv)
				.next();
	}

	
	
	private Map<Integer,String> mapHeadersToIndex(String[] headers) {
		
		int index = 0;
		for (String head : headers) {
			headerIndexMap.put(Integer.valueOf(index), head);
			index++;
		}
		Log.info("Header Map Parsed as "+headerIndexMap);
		headerMapped=true;
		return headerIndexMap;
	}
	
	
	public String getHeaderForIndex(int index,String[] headers) {
		if(!headerMapped){
			mapHeadersToIndex(headers);
		}
		return headerIndexMap.get(Integer.valueOf(index))!=null?headerIndexMap.get(Integer.valueOf(index)):"COL_"+index;
	}

}
