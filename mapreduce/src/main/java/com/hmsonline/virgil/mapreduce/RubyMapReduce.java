package com.hmsonline.virgil.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jruby.RubyArray;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.ScriptingContainer;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Map/Reduce Job
 */
public class RubyMapReduce extends Configured implements Tool {
    private static int MAX_COLUMNS_PER_ROW = 1000;
    
    public static void main(String[] args) {
        Configuration conf = JobSpawner.getConfiguration(args);
        try {
            ToolRunner.run(conf, new RubyMapReduce(), new String[0]);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static List<String> getGemPaths() {        
        // TODO: Make this load the gems dynamically from the filesystem
        List<String> paths = new ArrayList<String>();
        paths.add("gems/json-1.6.5-java/lib/");
        paths.add("gems/rest-client-1.6.7/lib/");
        paths.add("gems/mime-types-1.17.2/lib/");
        paths.add("gems/jruby-openssl-0.7.5/lib/shared/");
        paths.add("gems/bouncy-castle-java-1.5.0146.1/lib/");
        return paths;
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), getConf().get("jobName"));
        
        job.setJarByClass(RubyMapReduce.class);
        ((JobConf) job.getConfiguration()).setJar("mapreduce/jars/virgil-mapreduce-hdeploy.jar");
        job.setMapperClass(CassandraMapper.class);
        job.setReducerClass(CassandraReducer.class);
        job.setInputFormatClass(ColumnFamilyInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ObjectWritable.class);

        ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
        ConfigHelper.setInitialAddress(job.getConfiguration(), getConf().get("cassandraHost"));
        ConfigHelper.setRpcPort(job.getConfiguration(), getConf().get("cassandraPort"));

        ConfigHelper.setInputColumnFamily(job.getConfiguration(), getConf().get("inputKeyspace"),
                getConf().get("inputColumnFamily"));
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), getConf().get("outputKeyspace"),
                getConf().get("outputColumnFamily"));
        job.getConfiguration().set("source", getConf().get("source"));
        if (getConf().get("params") != null){
            job.getConfiguration().set("params", getConf().get("params"));
        }
        if (StringUtils.isNotBlank(getConf().get(JobSpawner.MAP_EMIT_FLAG_STR))){
          job.getConfiguration().set(JobSpawner.MAP_EMIT_FLAG_STR, getConf().get(JobSpawner.MAP_EMIT_FLAG_STR));
        }
        if (StringUtils.isNotBlank(getConf().get(JobSpawner.REDUCE_RAW_DATA_FLAG_STR))){
          job.getConfiguration().set(JobSpawner.REDUCE_RAW_DATA_FLAG_STR, getConf().get(JobSpawner.REDUCE_RAW_DATA_FLAG_STR));
        }
        job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
        SlicePredicate sp = new SlicePredicate();
        SliceRange sr = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false,
                MAX_COLUMNS_PER_ROW);
        sp.setSlice_range(sr);
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), sp);
        //job.waitForCompletion(true);
        job.submit();
        return 0;
    }

    public static class CassandraMapper extends
          Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, ObjectWritable> {
    private ScriptingContainer rubyContainer = null;
    private Object rubyReceiver = null;
    private Map<String, Object> params;
    private static Logger logger = LoggerFactory.getLogger(CassandraMapper.class);
    private Emitter emitter;

    @Override
    protected void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> value, Context context)
            throws IOException,
            InterruptedException {
      if (emitter != null) {
        mapRubyEmit(key, value, context);
      } else {
        mapJavaEmit(key, value, context);
    }
      }
    
    protected void mapJavaEmit(ByteBuffer key, SortedMap<ByteBuffer, IColumn> value, Context context) throws IOException,
    InterruptedException {
Map<String, String> columns = new HashMap<String, String>();
for (ByteBuffer b : value.keySet()) {
    columns.put(ByteBufferUtil.string(b), ByteBufferUtil.string(value.get(b).value()));
}
String rowKey = ByteBufferUtil.string(key);
try {
    RubyArray tuples = RubyInvoker.invokeMap(rubyContainer, rubyReceiver, rowKey, columns, params);
    for (Object element : tuples) {
        RubyArray tuple = (RubyArray) element;
        context.write(new Text((String) tuple.get(0)), new ObjectWritable(tuple.get(1)));
    }
} catch (Exception e) {
    // TODO: Make this more severe.
    logger.warn("Exception running map on [" + rowKey + "]", e);
}
}

    protected void mapRubyEmit(ByteBuffer key, SortedMap<ByteBuffer, IColumn> value, Context context)
            throws IOException,
            InterruptedException {
      Map<String, String> columns = new HashMap<String, String>();
      for (ByteBuffer b : value.keySet()) {
        columns.put(ByteBufferUtil.string(b), ByteBufferUtil.string(value.get(b).value()));
      }
      String rowKey = ByteBufferUtil.string(key);
      try {
        RubyInvoker.invokeMap(rubyContainer, rubyReceiver, rowKey, columns, this.emitter, params);
      }
      catch (Exception e) {
        // TODO: Make this more severe.
        logger.warn("Exception running map on [" + rowKey + "]", e);
      }
    }

        @SuppressWarnings("unchecked")
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String source = context.getConfiguration().get("source");
            this.rubyContainer = new ScriptingContainer(LocalContextScope.CONCURRENT);
            this.rubyContainer.setLoadPaths(getGemPaths());
            this.rubyReceiver = rubyContainer.runScriptlet(source);
            if (context.getConfiguration().get("params") != null) {
                params = new HashMap<String, Object>();
                params = (Map<String, Object>) JSONValue.parse(context.getConfiguration().get("params"));
            }
            if(StringUtils.isNotBlank(context.getConfiguration().get(com.hmsonline.virgil.mapreduce.JobSpawner.MAP_EMIT_FLAG_STR))) {
              emitter = new Emitter(context);
            } else {
              emitter = null;
            }
        }
    }

    public static class CassandraReducer extends Reducer<Text, ObjectWritable, ByteBuffer, List<Mutation>> {
        private ScriptingContainer rubyContainer = null;
        private Object rubyReceiver = null;
        private Map<String, Object> params;

        private boolean reduceRawDataFlag;

        @Override
        protected void reduce(Text key, Iterable<ObjectWritable> vals, Context context) throws IOException,
        InterruptedException {
          if(reduceRawDataFlag) {
            reduceRawData(key, vals, context);
          } else {
            reduceNonRawData(key, vals, context);
          }
            
}
        
        protected void reduceRawData(Text key, Iterable<ObjectWritable> vals, Context context) throws IOException,
        InterruptedException {
    try {
        Map<String, Map<String, String>> results = RubyInvoker.invokeReduce(rubyContainer, rubyReceiver,
                key.toString(), vals, params);
        for (String rowKey : results.keySet()) {
            Map<String, String> columns = results.get(rowKey);
            for (String columnName : columns.keySet()) {
                String columnValue = columns.get(columnName);
                context.write(ByteBufferUtil.bytes(rowKey),
                        CassandraReducer.getMutationList(columnName, columnValue));
            }
        }
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
}
        
        protected void reduceNonRawData(Text key, Iterable<ObjectWritable> vals, Context context) throws IOException,
                InterruptedException {
          List<Object> values = new ArrayList<Object>();
          for (ObjectWritable value : vals) {
              values.add(value.get());
          }
          try {
              Map<String, Map<String, String>> results = RubyInvoker.invokeReduce(rubyContainer, rubyReceiver,
                      key.toString(), values, params);
              for (String rowKey : results.keySet()) {
                  Map<String, String> columns = results.get(rowKey);
                  for (String columnName : columns.keySet()) {
                      String columnValue = columns.get(columnName);
                      context.write(ByteBufferUtil.bytes(rowKey),
                              CassandraReducer.getMutationList(columnName, columnValue));
                  }
              }
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String source = context.getConfiguration().get("source");
            this.rubyContainer = new ScriptingContainer(LocalContextScope.CONCURRENT);
            this.rubyContainer.setLoadPaths(getGemPaths());
            this.rubyReceiver = rubyContainer.runScriptlet(source);
            if (context.getConfiguration().get("params") != null) {
                params = new HashMap<String, Object>();
                params = (Map<String, Object>) JSONValue.parse(context.getConfiguration().get("params"));
            }
            reduceRawDataFlag = StringUtils.isNotBlank(context.getConfiguration().get(com.hmsonline.virgil.mapreduce.JobSpawner.REDUCE_RAW_DATA_FLAG_STR));
        }

        private static List<Mutation> getMutationList(String name, String value) {
            Column c = new Column();
            c.setName(Arrays.copyOf(name.getBytes(), name.length()));
            c.setValue(ByteBufferUtil.bytes(value.toString()));
            c.setTimestamp(System.currentTimeMillis() * 1000);

            Mutation m = new Mutation();
            m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
            m.column_or_supercolumn.setColumn(c);

            ArrayList<Mutation> retval = new ArrayList<Mutation>(1);
            retval.add(m);
            return retval;
        }
    }
}
