package org.apache.virgil.index;

import org.apache.virgil.index.Indexer;
import org.apache.virgil.index.SolrIndexer;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class IndexerTest {
    private static final String COLUMN_FAMILY = "TEST_CF";
    private static final String KEY = "TEST_ROW";

	@Test
	public void testIndexing() throws Exception {
		Indexer indexer = new SolrIndexer(null);
		String json = "{\"ADDR1\":\"1234 Collin St.\",\"CITY\":\"Souderton\"}";
		indexer.index(COLUMN_FAMILY, KEY, json);

		json = "{\"ADDR1\":\"1234 Owen St.\",\"CITY\":\"Pottstown\"}";
		indexer.index(COLUMN_FAMILY, KEY + "2", json);
		// TODO: Need to figure out how to test w/o a live SOLR
		indexer.delete(COLUMN_FAMILY, KEY);
	}
}
