package com.ak.mapreduce.orc.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class ORCReducer extends
		Reducer<Text, NullWritable, NullWritable, Writable> {

	
	private final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString("struct<key:string,age:int,name:string>");
	private final ObjectInspector oip = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
	private final OrcSerde serde = new OrcSerde(); 
	private final NullWritable nullWriteable = NullWritable.get();

	@Override
	protected void reduce(Text line, Iterable<NullWritable> arg1,
			Reducer<Text, NullWritable, NullWritable, Writable>.Context context)
			throws IOException, InterruptedException {

		String valueString = line.toString();
		String key = valueString.split(",")[0];
		int age = Integer.parseInt(valueString.split(",")[1].trim());
		String name = valueString.split(",")[2];
		
		List<Object> struct =new ArrayList<Object>(4);
		struct.add(0, key);
		struct.add(1, age);
		struct.add(2, name);
		
		Writable serialStruct = serde.serialize(struct, oip);
		context.write(nullWriteable, serialStruct);
	}

}
