package com.hmsonline.virgil.mapreduce;

import java.util.List;
import java.util.Map;

public interface VirgilMap {
	//  Takes a row and returns a set of key,value pairs
	//  Map<key, value> map (rowkey, map<column_name, column_value>)
	public Map<String, Object> map (String rowKey, Map<String, String> columns, Map<String, Object>  params);
	
	//  Takes a key and a list of values and returns a set of rows.
	//        Map<rowkeys, columns>     columns vals                   key     values
	public Map<String, Map<String,String>> reduce (String key, List<Object> values, Map<String, Object>  params);
}
