package org.apache.cassandra.http.index;

import org.apache.cassandra.http.VirgilConfig;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class SolrIndexer implements Indexer {
	public static String SOLR_CONFIG_PARAM = "solr_host";
	public static String CONTENT_TYPE = "application/json";
	public static String XML_CONTENT_TYPE = "application/xml";
	public static String CHAR_SET = "UTF8";
	// TODO: Maybe move dynamic field suffix to config file?
	public static String DYNAMIC_FIELD_SUFFIX = "_t";
	private String solrUrl = null;

	public SolrIndexer() {
		solrUrl = VirgilConfig.getValue(SOLR_CONFIG_PARAM);
	}

	@Override
	public void index(String columnFamily, String rowKey, String json) throws Exception {
		JSONObject row = (JSONObject) JSONValue.parse(json);
		index(columnFamily, rowKey, row);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void index(String columnFamily, String rowKey, JSONObject row) throws Exception {
		HttpClient client = new HttpClient();
		PostMethod post = new PostMethod(solrUrl + "update/json?commit=true");
		JSONObject document = new JSONObject();

		document.put("id", this.getDocumentId(columnFamily, rowKey));
		document.put("rowKey" + DYNAMIC_FIELD_SUFFIX, rowKey);
		document.put("columnFamily" + DYNAMIC_FIELD_SUFFIX, columnFamily);
		for (Object column : row.keySet()) {
			document.put(column.toString().toLowerCase() + DYNAMIC_FIELD_SUFFIX, row.get(column));
		}

		// Index
		RequestEntity requestEntity = new StringRequestEntity("[" + document.toString() + "]", CONTENT_TYPE, CHAR_SET);
		post.setRequestEntity(requestEntity);
		try {
			client.executeMethod(post);
		} finally {
			post.releaseConnection();
		}
	}

	@Override
	public void delete(String columnFamily, String rowKey) throws Exception {
		HttpClient client = new HttpClient();

		// Commit
		PostMethod post = new PostMethod(solrUrl + "update?commit=true");
		String query = "id:" + this.getDocumentId(columnFamily, rowKey);
		StringRequestEntity requestEntity = new StringRequestEntity("<delete><query>" + query + "</query></delete>",
				XML_CONTENT_TYPE, CHAR_SET);
		post.setRequestEntity(requestEntity);
		client.executeMethod(post);
		post.releaseConnection();
	}

	// TODO: Could hash the rowkey and column then combine to avoid potential
	// collisions.
	private String getDocumentId(String columnFamily, String rowKey) {
		return columnFamily + "." + rowKey;
	}
}
