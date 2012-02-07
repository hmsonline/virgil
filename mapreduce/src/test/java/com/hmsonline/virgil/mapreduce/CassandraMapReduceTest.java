package com.hmsonline.virgil.mapreduce;

import org.junit.Ignore;
import org.junit.Test;

import com.hmsonline.virgil.mapreduce.JobSpawner;

@Ignore
public class CassandraMapReduceTest {
		@Test
	public void testMapReduce() throws Exception {
		String source = RubyInvokerTest.getSource(); 
		JobSpawner.spawnLocal("test-reduce", "localhost", 9160, "playground", "toys", "datastore", "test", source, null, null, null);
	}

}
