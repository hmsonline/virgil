package org.apache.cassandra.http.mapreduce;

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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
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
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), args[JobSpawner.JOB_NAME]);
		job.setJarByClass(RubyMapReduce.class);
		job.setMapperClass(CassandraMapper.class);
		job.setReducerClass(CassandraReducer.class);
		job.setInputFormatClass(ColumnFamilyInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ObjectWritable.class);

		ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
		ConfigHelper.setInitialAddress(job.getConfiguration(), args[JobSpawner.CASSANDRA_HOST]);
		ConfigHelper.setRpcPort(job.getConfiguration(), args[JobSpawner.CASSANDRA_PORT]);
		
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), args[JobSpawner.INPUT_KEYSPACE], args[JobSpawner.INPUT_COLUMN_FAMILY]);
		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), args[JobSpawner.OUTPUT_KEYSPACE], args[JobSpawner.OUTPUT_COLUMN_FAMILY]);
		job.getConfiguration().set("source", args[JobSpawner.SOURCE]);
		job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
		SlicePredicate sp = new SlicePredicate();
		SliceRange sr = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, MAX_COLUMNS_PER_ROW);
		sp.setSlice_range(sr);
		ConfigHelper.setInputSlicePredicate(job.getConfiguration(), sp);
		job.waitForCompletion(true);
		return 0;
	}

	public static class CassandraMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, ObjectWritable> {
		private ScriptingContainer rubyContainer = null;
		private Object rubyReceiver = null;
		private Map<String, Object> params;
		private static Logger logger = LoggerFactory.getLogger(CassandraMapper.class);

    @Override
		protected void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> value, Context context) throws IOException,
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
				logger.warn("Exception running map on [" + rowKey +"]", e);
			}
		}

    @SuppressWarnings("unchecked")
	  @Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String source = context.getConfiguration().get("source");
			this.rubyContainer = new ScriptingContainer(LocalContextScope.CONCURRENT);
			this.rubyReceiver = rubyContainer.runScriptlet(source);
		  if (context.getConfiguration().get("params") != null) {
        params = new HashMap<String, Object>();
        params = (Map<String, Object>) JSONValue.parse(context.getConfiguration().get("params"));        
      }
		}
	}

	public static class CassandraReducer extends Reducer<Text, ObjectWritable, ByteBuffer, List<Mutation>> {
		private ScriptingContainer rubyContainer = null;
		private Object rubyReceiver = null;
	  private Map<String, Object> params;

    @Override
		protected void reduce(Text key, Iterable<ObjectWritable> vals, Context context) throws IOException, InterruptedException {
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
			this.rubyReceiver = rubyContainer.runScriptlet(source);
      if (context.getConfiguration().get("params") != null) {
        params = new HashMap<String, Object>();
        params = (Map<String, Object>) JSONValue.parse(context.getConfiguration().get("params"));
      }
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
