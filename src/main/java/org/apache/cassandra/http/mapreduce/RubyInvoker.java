package org.apache.cassandra.http.mapreduce;

import java.util.List;
import java.util.Map;

import javax.script.ScriptException;

import org.jruby.RubyArray;
import org.jruby.embed.ScriptingContainer;

public class RubyInvoker {

	public static synchronized RubyArray invokeMap(ScriptingContainer container, Object rubyReceiver,
			String rowKey, Map<String, String> columns) throws ScriptException {
		return (RubyArray) container.callMethod(rubyReceiver, "map", rowKey, columns);
	}

	@SuppressWarnings("unchecked")
	public static synchronized Map<String, Map<String, String>> invokeReduce(ScriptingContainer container,
			Object rubyReceiver, String key, List<String> values) throws ScriptException {
		return (Map<String, Map<String, String>>) container.callMethod(rubyReceiver, "reduce", key, values);
	}
}
