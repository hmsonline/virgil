package org.apache.cassandra.http.index;

public interface Indexer {

	/** 
	 * Indexes the content passed into the method,
	 * Assumes single-level/flat structure of the JSON.
	 * @param json
	 */
	public void index(String columnFamily, String rowKey, String json) throws Exception;
	
	/** 
	 * Removes a row from the index.
	 */
	public void delete(String columnFamily, String rowKey) throws Exception;
}
