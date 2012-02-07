package com.hmsonline.virgil.mapreduce;

import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class JavascriptInvoker {

	public static synchronized String[][] invokeMap(String script, String rowKey, Map<String, String> columns)
			throws ScriptException {
		ScriptEngineManager manager = new ScriptEngineManager();
		ScriptEngine jsEngine = manager.getEngineByExtension("js");
		//String value = (String) jsEngine.getFactory().getParameter("THREADING");
		//Compilable jsCompile = (Compilable) jsEngine;
		//CompiledScript run = jsCompile.compile(script);

		jsEngine.eval(script);
		jsEngine.put("rowKey", "foo");
		jsEngine.put("columns", columns);
		jsEngine.eval("map(rowKey, columns)");
		return null;
	}

}
