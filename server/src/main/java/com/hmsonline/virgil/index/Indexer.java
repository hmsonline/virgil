package com.hmsonline.virgil.index;

import java.io.IOException;

import org.apache.commons.httpclient.HttpException;
import org.json.simple.JSONObject;

public interface Indexer {

    /**
     * Indexes the content passed into the method, Assumes single-level/flat
     * structure of the JSON.
     * 
     * @param json
     */
    public void index(String columnFamily, String rowKey, String json) throws HttpException, IOException;

    public void index(String columnFamily, String rowKey, JSONObject json) throws HttpException, IOException;

    /**
     * Removes a row from the index.
     */
    public void delete(String columnFamily, String rowKey) throws HttpException, IOException;
}
