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
    public static final int PARAMS = 10;

    private static String[] getArgs(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
            String inputColumnFamily, String outputKeyspace, String outputColumnFamily, String source, String params) {
        List<String> args = new ArrayList<String>();
        args.add("mapreduce/jars/virgil-mapreduce-hdeploy.jar");
        args.add("org.apache.cassandra.http.mapreduce.RubyMapReduce");
        args.add(jobName);
        args.add(cassandraHost);
        args.add(Integer.toString(cassandraPort));
        args.add(inputKeyspace);
        args.add(inputColumnFamily);
        args.add(outputKeyspace);
        args.add(outputColumnFamily);
        args.add(source);
        args.add(params);
        LOG.info("Running job against [" + cassandraHost + ":" + cassandraPort + "]");
        return args.toArray(new String[0]);
    }

    public static Configuration getConfiguration(String[] args){
        for (int i=0; i < args.length; i++){
            System.out.println("[" + i + "] = [" + args[i] + "]");
        }
//        System.out.println("Input --> [" + args[JobSpawner.INPUT_KEYSPACE] + "]:[" + args[JobSpawner.INPUT_COLUMN_FAMILY] + "]");
//        System.out.println("Output <-- [" + args[JobSpawner.OUTPUT_KEYSPACE] + "]:[" + args[JobSpawner.OUTPUT_COLUMN_FAMILY] + "]");

        Configuration conf = new Configuration();
        conf.set("jobName", args[JobSpawner.JOB_NAME - 2]);
        conf.set("cassandraHost", args[JobSpawner.CASSANDRA_HOST - 2]);
        conf.set("cassandraPort", args[JobSpawner.CASSANDRA_PORT - 2]);
        conf.set("inputKeyspace", args[JobSpawner.INPUT_KEYSPACE - 2]);
        conf.set("inputColumnFamily", args[JobSpawner.INPUT_COLUMN_FAMILY - 2]);
        conf.set("outputKeyspace", args[JobSpawner.OUTPUT_KEYSPACE - 2]);
        conf.set("outputColumnFamily", args[JobSpawner.OUTPUT_COLUMN_FAMILY - 2]);
        conf.set("source", args[JobSpawner.SOURCE - 2]);
        if (StringUtils.isNotBlank(args[JobSpawner.PARAMS - 2])) {
            conf.set("params", args[JobSpawner.PARAMS - 2]);
        }
        return conf;
    }
    
    public static void spawnLocal(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
            String inputColumnFamily, String outputKeyspace, String outputColumnFamily, String source, String params)
            throws Exception {
        String[] args = JobSpawner.getArgs(jobName, cassandraHost, cassandraPort, inputKeyspace, inputColumnFamily,
                outputKeyspace, outputColumnFamily, source, params);
        Configuration conf = JobSpawner.getConfiguration(args);
        ToolRunner.run(conf, new RubyMapReduce(), new String[0]);
    }

    public static void spawnRemote(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
            String inputColumnFamily, String outputKeyspace, String outputColumnFamily, String source, String params)
            throws Throwable {
        String[] args = JobSpawner.getArgs(jobName, cassandraHost, cassandraPort, inputKeyspace, inputColumnFamily,
                outputKeyspace, outputColumnFamily, source, params);
        RunJar.main(args);
    }
}
