package org.apache.cassandra.http.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class JobSpawner {


	public static void spawn(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
			String inputColumnFamily, String outputKeyspace, String outputColumnFamily, String source, String params) throws Exception {
		Configuration conf = new Configuration();
		conf.set("jobName", jobName);
		conf.set("cassandraHost", cassandraHost);
		conf.set("cassandraPort", Integer.toString(cassandraPort));
		conf.set("inputKeyspace", inputKeyspace);
		conf.set("inputColumnFamily", inputColumnFamily);
		conf.set("outputKeyspace", outputKeyspace);
		conf.set("outputColumnFamily", outputColumnFamily);
		conf.set("source", source);
		if (StringUtils.isNotBlank(params)) {
		  conf.set("params", params);
		}
		ToolRunner.run(conf, new RubyMapReduce(), null);
	}
}
