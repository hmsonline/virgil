package org.apache.cassandra.http.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class JobSpawner {


	public static void spawn(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
			String inputColumnFamily, String outputKeyspace, String outputColumnFamily, String source) throws Exception {
		Configuration conf = new Configuration();
		conf.set("jobName", jobName);
		conf.set("cassandraHost", cassandraHost);
		conf.set("cassandraPort", Integer.toString(cassandraPort));
		conf.set("inputKeyspace", inputKeyspace);
		conf.set("inputColumnFamily", inputColumnFamily);
		conf.set("outputKeyspace", outputKeyspace);
		conf.set("outputColumnFamily", outputColumnFamily);
		conf.set("source", source);
		ToolRunner.run(conf, new RubyMapReduce(), null);
	}
}
