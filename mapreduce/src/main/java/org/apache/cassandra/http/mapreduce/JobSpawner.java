package org.apache.cassandra.http.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobSpawner {
	private static final Logger LOG = LoggerFactory.getLogger(JobSpawner.class);
	public static final int JOB_JAR_FILE = 0;
	public static final int JOB_CLASS_NAME = 1;
	public static final int JOB_NAME = 2;
	public static final int CASSANDRA_HOST = 3;
	public static final int CASSANDRA_PORT = 4;
	public static final int INPUT_KEYSPACE = 5;
	public static final int INPUT_COLUMN_FAMILY = 6;
	public static final int OUTPUT_KEYSPACE = 7;
	public static final int OUTPUT_COLUMN_FAMILY = 8;
	public static final int SOURCE = 9;

	private static String[] getArgs(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
			String inputColumnFamily, String outputKeyspace, String outputColumnFamily, String source){
		List<String> args = new ArrayList<String>();
		args.add("mapreduce/jars/virgil-mapreduce-hdeploy.jar");
		args.add("com.apache.cassandra.http.mapreduce.RubyMapReduce");
		args.add(jobName);
		args.add(cassandraHost);
		args.add(Integer.toString(cassandraPort));
		args.add(inputKeyspace);
		args.add(inputColumnFamily);
		args.add(outputKeyspace);
		args.add(outputColumnFamily);
		args.add(source);
		
		LOG.info("Running job against [" + cassandraHost + ":" + cassandraPort + "]");
		
		return args.toArray(new String[0]);
	}	

	public static void spawnLocal(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
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
        String[] args = JobSpawner.getArgs(jobName, cassandraHost, cassandraPort, inputKeyspace, inputColumnFamily, 
                outputKeyspace, outputColumnFamily, source);
		ToolRunner.run(conf, new RubyMapReduce(), args);
	}
	
	public static void spawnRemote(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
			String inputColumnFamily, String outputKeyspace, String outputColumnFamily, String source) throws Throwable {
		String[] args = JobSpawner.getArgs(jobName, cassandraHost, cassandraPort, inputKeyspace, inputColumnFamily, 
				outputKeyspace, outputColumnFamily, source);
		RunJar.main(args);		
	}
}
