package com.hmsonline.virgil.index;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.hmsonline.virgil.config.VirgilConfiguration;

public class SolrIndexer implements Indexer {
    public static String SOLR_CONFIG_PARAM = "solr_host";
    public static String CONTENT_TYPE = "application/json";
    public static String XML_CONTENT_TYPE = "application/xml";
    public static String CHAR_SET = "UTF8";
    // TODO: Maybe move dynamic field suffix to config file?
    public static String DYNAMIC_FIELD_SUFFIX = "_t";
    private String solrUrl = null;

    public SolrIndexer(VirgilConfiguration config) {
        solrUrl = config.getSolrHost();
    }

    public void index(String columnFamily, String rowKey, String json) throws HttpException, IOException  {
        JSONObject row = (JSONObject) JSONValue.parse(json);
        index(columnFamily, rowKey, row);
    }

    @SuppressWarnings("unchecked")
    public void index(String columnFamily, String rowKey, JSONObject row) throws HttpException, IOException {
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

    public void delete(String columnFamily, String rowKey) throws HttpException, IOException {
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
