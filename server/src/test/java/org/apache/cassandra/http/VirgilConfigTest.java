package org.apache.cassandra.http;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class VirgilConfigTest {

	@Test
	public void testConfig() throws Exception {
		assertEquals("http://localhost:8983/solr/", VirgilConfig.getValue("solr_host"));
	}
}
