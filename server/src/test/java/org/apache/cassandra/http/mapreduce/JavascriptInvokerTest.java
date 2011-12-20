package org.apache.cassandra.http.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class JavascriptInvokerTest {
	@Test
	public void testMapInvocation() throws Exception {
		String source = getSource();
		//System.out.println(source);

		Map<String, String> columns = new HashMap<String, String>();
		columns.put("collin", "42");
		columns.put("owen", "33");

		JavascriptInvoker.invokeMap(source, "row", columns);
	}

	public static String getSource() throws IOException {
		InputStream is = Test.class.getResourceAsStream("/wordcount.js");
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
