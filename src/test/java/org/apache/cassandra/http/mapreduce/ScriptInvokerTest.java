package org.apache.cassandra.http.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jruby.RubyArray;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.ScriptingContainer;
import org.junit.Test;

public class ScriptInvokerTest {
	@Test
	public void testMapInvocation() throws Exception {
		String source = getSource();
		System.out.println(source);
		ScriptingContainer rubyContainer = new ScriptingContainer(LocalContextScope.CONCURRENT);
		Object rubyReceiver = rubyContainer.runScriptlet(source);

		Map<String, String> columns = new HashMap<String, String>();
		columns.put("collin", "42");
		columns.put("owen", "33");

		RubyArray tuples = ScriptInvoker.invokeMap(rubyContainer, rubyReceiver, "rockwall", columns);
		assertEquals(2, tuples.size());
	}

	@Test
	public void testReduceInvocation() throws Exception {
		String source = getSource();
		ScriptingContainer rubyContainer = new ScriptingContainer(LocalContextScope.CONCURRENT);
		Object rubyReceiver = rubyContainer.runScriptlet(source);
		String key = "fun";
		List<String> values = new ArrayList<String>();
		values.add("1");
		values.add("3");
		values.add("5");
		Map<String, Map<String, String>> results = ScriptInvoker.invokeReduce(rubyContainer, rubyReceiver, key, values);
		assertEquals(3, results.size());
	}

	public static String getSource() throws IOException {
		InputStream is = Test.class.getResourceAsStream("/wordcount.rb");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();
		String line = null;
		while ((line = reader.readLine()) != null) {
			sb.append(line + "\n");
		}
		is.close();
		return sb.toString();
	}
}
