package org.apache.virgil.mapreduce;

import org.apache.virgil.mapreduce.JobSpawner;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class CassandraMapReduceTest {
		@Test
	public void testMapReduce() throws Exception {
		String source = RubyInvokerTest.getSource(); 
		JobSpawner.spawnLocal("test-reduce", "localhost", 9160, "playground", "toys", "datastore", "test", source, null, null, null);
	}

}
