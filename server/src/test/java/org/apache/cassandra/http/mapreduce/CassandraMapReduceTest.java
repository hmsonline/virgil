package org.apache.cassandra.http.mapreduce;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class CassandraMapReduceTest {
		@Test
	public void testMapReduce() throws Exception {
		String source = RubyInvokerTest.getSource(); 
		JobSpawner.spawn("test-reduce", "localhost", 9160, "playground", "toys", "datastore", "test", source);
	}

}
