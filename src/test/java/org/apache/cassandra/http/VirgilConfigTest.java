package org.apache.cassandra.http;

import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class VirgilConfigTest {

	@Test
	public void testConfig() throws Exception {
		Map<String,String> configuration = VirgilConfig.getConfig();
		assertEquals("http://localhost:8983/", configuration.get("solr_host"));
	}
}
