package org.apache.cassandra.http;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.KsDef;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JsonMarshaller {

	@SuppressWarnings("unchecked")
	public static String marshallColumn(ColumnOrSuperColumn column) throws UnsupportedEncodingException {
		JSONObject json = new JSONObject();
		Column c = column.getColumn();
		json.put(string(c.getName()), string(c.getValue()));
		return json.toString();
	}

	@SuppressWarnings("unchecked")
	public static String marshallSlice(List<ColumnOrSuperColumn> slice) throws UnsupportedEncodingException {
		JSONObject json = new JSONObject();
		for (ColumnOrSuperColumn column : slice) {
			Column c = column.getColumn();
			json.put(string(c.getName()), string(c.getValue()));
		}
		return json.toString();
	}

	@SuppressWarnings("unchecked")
	// TODO: Make this hierarchical, then flatten when necessary
	public static String marshallKeyspaces(List<KsDef> keyspaces, boolean flatten) throws UnsupportedEncodingException {
		JSONArray keyspaceJson = new JSONArray();
		if (flatten) {
			for (KsDef keyspace : keyspaces) {
				List<CfDef> columnFamilies = keyspace.getCf_defs();
				for (CfDef columnFamily : columnFamilies) {
					JSONObject json = new JSONObject();
					json.put("keyspace", keyspace.getName());
					json.put("columnFamily", columnFamily.getName());
					keyspaceJson.add(json);
				}
			}
		} else {
			for (KsDef keyspace : keyspaces) {
				JSONObject json = new JSONObject();
				json.put("keyspace", keyspace.getName());
				json.put("strategy", keyspace.getStrategy_class());
				List<CfDef> columnFamilies = keyspace.getCf_defs();
				JSONArray cfJsonArray = new JSONArray();
				for (CfDef columnFamily : columnFamilies) {
					JSONObject cfJson = new JSONObject();
					cfJson.put("name", columnFamily.getName());
					cfJsonArray.add(cfJson);
				}
				json.put("columnFamilies", cfJsonArray);
				keyspaceJson.add(json);
			}
		}
		return keyspaceJson.toString();
	}

	private static String string(byte[] bytes) throws UnsupportedEncodingException {
		return new String(bytes, "UTF8");
	}

}

/* TOMBSTONE */

/*
 * 
 * public static JSONArray flatten (JSONArray original) { JSONArray flatJson =
 * new JSONArray(); JSONObject parentAttributes = new JSONObject();
 * JsonMarshaller.flattenHelper(parentAttributes, original, flatJson); return
 * flatJson; }
 * 
 * @SuppressWarnings("unchecked") public static void flattenHelper (JSONObject
 * parentAttributes, Object node, JSONArray flatJson) { if (node instanceof
 * JSONArray){ JSONArray array = (JSONArray) node; Iterator<JSONObject> iter =
 * array.iterator(); while(iter.hasNext()){ Object child = iter.next();
 * JsonMarshaller.flattenHelper(parentAttributes, child, flatJson); } } }
 */
