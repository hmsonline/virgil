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
import org.apache.cassandra.http.CassandraRestService;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jruby.RubyArray;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Map/Reduce Job
 */
public class RubyMapReduce extends Configured implements Tool {
	private static int MAX_COLUMNS_PER_ROW = 1000;

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

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), getConf().get("jobName"));
		job.setJarByClass(RubyMapReduce.class);
		job.setMapperClass(CassandraMapper.class);
		job.setReducerClass(CassandraReducer.class);
		job.setInputFormatClass(ColumnFamilyInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		ConfigHelper.setRpcPort(job.getConfiguration(), getConf().get("cassandraPort"));
		ConfigHelper.setInitialAddress(job.getConfiguration(), getConf().get("cassandraHost"));
		ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), getConf().get("inputKeyspace"),
				getConf().get("inputColumnFamily"));

		ConfigHelper.setOutputColumnFamily(job.getConfiguration(), getConf().get("outputKeyspace"),
				getConf().get("outputColumnFamily"));
		job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
		SlicePredicate sp = new SlicePredicate();
		SliceRange sr = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false,
				MAX_COLUMNS_PER_ROW);
		sp.setSlice_range(sr);
		ConfigHelper.setInputSlicePredicate(job.getConfiguration(), sp);

		job.waitForCompletion(true);
		return 0;
	}

	public static class CassandraMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text> {
		private ScriptingContainer rubyContainer = null;
		private Object rubyReceiver = null;
		private static Logger logger = LoggerFactory.getLogger(CassandraRestService.class);

		@Override
		protected void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> value, Context context) throws IOException,
				InterruptedException {
			Map<String, String> columns = new HashMap<String, String>();
			for (ByteBuffer b : value.keySet()) {
				columns.put(ByteBufferUtil.string(b), ByteBufferUtil.string(value.get(b).value()));
			}
			String rowKey = ByteBufferUtil.string(key);
			try {
				RubyArray tuples = RubyInvoker.invokeMap(rubyContainer, rubyReceiver, rowKey, columns);
				for (Object element : tuples) {
					RubyArray tuple = (RubyArray) element;
					context.write(new Text((String) tuple.get(0)), new Text((String) tuple.get(1)));
				}
			} catch (Exception e) {
				// TODO: Make this more severe.
				logger.warn("Exception running map on [" + rowKey +"]", e);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String source = context.getConfiguration().get("source");
			this.rubyContainer = new ScriptingContainer(LocalContextScope.CONCURRENT);
			this.rubyReceiver = rubyContainer.runScriptlet(source);
		}
	}

	public static class CassandraReducer extends Reducer<Text, Text, ByteBuffer, List<Mutation>> {
		private ScriptingContainer rubyContainer = null;
		private Object rubyReceiver = null;

		@Override
		protected void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			List<String> values = new ArrayList<String>();
			for (Text value : vals) {
				values.add(value.toString());
			}
			try {
				Map<String, Map<String, String>> results = RubyInvoker.invokeReduce(rubyContainer, rubyReceiver,
						key.toString(), values);
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

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String source = context.getConfiguration().get("source");
			this.rubyContainer = new ScriptingContainer(LocalContextScope.CONCURRENT);
			this.rubyReceiver = rubyContainer.runScriptlet(source);
		}

		private static List<Mutation> getMutationList(String name, String value) {
			Column c = new Column();
			c.setName(Arrays.copyOf(name.getBytes(), name.length()));
			c.setValue(ByteBufferUtil.bytes(value.toString()));
			c.setTimestamp(System.currentTimeMillis());

			Mutation m = new Mutation();
			m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
			m.column_or_supercolumn.setColumn(c);

			ArrayList<Mutation> retval = new ArrayList<Mutation>(1);
			retval.add(m);
			return retval;
		}
	}
}
