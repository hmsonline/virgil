package com.hmsonline.virgil.mapreduce;

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
    public static final int JOB_NAME = 0;
    public static final int CASSANDRA_HOST = 1;
    public static final int CASSANDRA_PORT = 2;
    public static final int INPUT_KEYSPACE = 3;
    public static final int INPUT_COLUMN_FAMILY = 4;
    public static final int OUTPUT_KEYSPACE = 5;
    public static final int OUTPUT_COLUMN_FAMILY = 6;
    public static final int SOURCE = 7;
    public static final int PARAMS = 8;
    public static final int MAP_EMIT_FLAG = 9;
    public static final int REDUCE_RAW_DATA_FLAG = 10;
    
    public static final String MAP_EMIT_FLAG_STR = "mapEmitFlag";
    public static final String REDUCE_RAW_DATA_FLAG_STR = "reduceRawDataFlag";

  private static String[] getArgs(String jobName, String cassandraHost, int cassandraPort,
                                  String inputKeyspace,
                                  String inputColumnFamily, String outputKeyspace,
                                  String outputColumnFamily, String source, String params,
                                  String mapEmitFlag,
                                  String reduceRawDataFlag,
                                  boolean local) {
        List<String> args = new ArrayList<String>();
        if (!local) {
            args.add("mapreduce/jars/virgil-mapreduce-hdeploy.jar");
            args.add("com.hmsonline.virgil.mapreduce.RubyMapReduce");
        }
        args.add(jobName);
        args.add(cassandraHost);
        args.add(Integer.toString(cassandraPort));
        args.add(inputKeyspace);
        args.add(inputColumnFamily);
        args.add(outputKeyspace);
        args.add(outputColumnFamily);
        args.add(source);
        args.add(params);
        args.add(mapEmitFlag);
        args.add(reduceRawDataFlag);
        LOG.info("Running job against [" + cassandraHost + ":" + cassandraPort + "]");
        return args.toArray(new String[0]);
    }

    public static Configuration getConfiguration(String[] args) {
        LOG.debug("Starting [" + args[JobSpawner.JOB_NAME] + "] against Cassandra ["
                + args[JobSpawner.CASSANDRA_HOST] + ":" + args[JobSpawner.CASSANDRA_PORT] + "]");
        LOG.debug("Input --> [" + args[JobSpawner.INPUT_KEYSPACE] + "]:["
                + args[JobSpawner.INPUT_COLUMN_FAMILY] + "]");
        LOG.debug("Output <-- [" + args[JobSpawner.OUTPUT_KEYSPACE] + "]:["
                + args[JobSpawner.OUTPUT_COLUMN_FAMILY] + "]");
        Configuration conf = new Configuration();
        conf.set("jobName", args[JobSpawner.JOB_NAME]);
        conf.set("cassandraHost", args[JobSpawner.CASSANDRA_HOST]);
        conf.set("cassandraPort", args[JobSpawner.CASSANDRA_PORT]);
        conf.set("inputKeyspace", args[JobSpawner.INPUT_KEYSPACE]);
        conf.set("inputColumnFamily", args[JobSpawner.INPUT_COLUMN_FAMILY]);
        conf.set("outputKeyspace", args[JobSpawner.OUTPUT_KEYSPACE]);
        conf.set("outputColumnFamily", args[JobSpawner.OUTPUT_COLUMN_FAMILY]);
        conf.set("source", args[JobSpawner.SOURCE]);
        if (args.length > JobSpawner.PARAMS && StringUtils.isNotBlank(args[JobSpawner.PARAMS])) {
            conf.set("params", args[JobSpawner.PARAMS]);
        }
        if (StringUtils.isNotBlank(args[MAP_EMIT_FLAG])){
          conf.set(MAP_EMIT_FLAG_STR, args[MAP_EMIT_FLAG]);
        }
        if (StringUtils.isNotBlank(args[MAP_EMIT_FLAG])){
          conf.set(MAP_EMIT_FLAG_STR, args[MAP_EMIT_FLAG]);
        }
        if (StringUtils.isNotBlank(args[REDUCE_RAW_DATA_FLAG])){
          conf.set(REDUCE_RAW_DATA_FLAG_STR, args[REDUCE_RAW_DATA_FLAG]);
        }
        return conf;
    }

    public static void spawnLocal(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
            String inputColumnFamily, String outputKeyspace, String outputColumnFamily, String source, String params, String mapEmitFlag, String reduceRawDataFlag)
            throws Exception {
        String[] args = JobSpawner.getArgs(jobName, cassandraHost, cassandraPort, inputKeyspace, inputColumnFamily,
                outputKeyspace, outputColumnFamily, source, params, mapEmitFlag, reduceRawDataFlag, true);
        Configuration conf = JobSpawner.getConfiguration(args);
        ToolRunner.run(conf, new RubyMapReduce(), new String[0]);
    }

    public static void spawnRemote(String jobName, String cassandraHost, int cassandraPort, String inputKeyspace,
            String inputColumnFamily, String outputKeyspace, String outputColumnFamily, String source, String params, String mapEmitFlag, String reduceRawDataFlag)
            throws Throwable {
        String[] args = JobSpawner.getArgs(jobName, cassandraHost, cassandraPort, inputKeyspace, inputColumnFamily,
                outputKeyspace, outputColumnFamily, source, params, mapEmitFlag, reduceRawDataFlag, false);
        RunJar.main(args);
    }
}
