package com.hmsonline.virgil.mapreduce;

import java.util.List;
import java.util.Map;

import javax.script.ScriptException;

import org.apache.hadoop.io.ObjectWritable;
import org.jruby.RubyArray;
import org.jruby.embed.ScriptingContainer;

public class RubyInvoker {    
    public static synchronized RubyArray invokeMap(ScriptingContainer container, Object rubyReceiver, String rowKey,
            Map<String, String> columns, Emitter emitter, Map<String, Object> params) throws ScriptException {
        // TODO- see if we can possibly deprecate the one without params
        if (params == null) { 
            return (RubyArray) container.callMethod(rubyReceiver, "map", rowKey, columns, emitter);
        } else {
            return (RubyArray) container.callMethod(rubyReceiver, "map", rowKey, columns, emitter, params);
        }
    }
    
    public static synchronized RubyArray invokeMap(ScriptingContainer container, Object rubyReceiver, String rowKey,
                                                   Map<String, String> columns, Map<String, Object> params) throws ScriptException {
                                               // TODO- see if we can possibly deprecate the one without params
                                               if (params == null) { 
                                                   return (RubyArray) container.callMethod(rubyReceiver, "map", rowKey, columns);
                                               } else {
                                                   return (RubyArray) container.callMethod(rubyReceiver, "map", rowKey, columns, params);
                                               }
                                           }

    @SuppressWarnings("unchecked")
    public static synchronized Map<String, Map<String, String>> invokeReduce(ScriptingContainer container,
            Object rubyReceiver, String key, Iterable<ObjectWritable> values, Map<String, Object> params) throws ScriptException {
        // TODO- see if we can possibly deprecate the one without params
        if (params == null) {
            return (Map<String, Map<String, String>>) container.callMethod(rubyReceiver, "reduce", key, values);
        } else {
            return (Map<String, Map<String, String>>) container.callMethod(rubyReceiver, "reduce", key, values, params);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static synchronized Map<String, Map<String, String>> invokeReduce(ScriptingContainer container,
            Object rubyReceiver, String key, List<Object> values, Map<String, Object> params) throws ScriptException {
        // TODO- see if we can possibly deprecate the one without params
        if (params == null) {
            return (Map<String, Map<String, String>>) container.callMethod(rubyReceiver, "reduce", key, values);
        } else {
            return (Map<String, Map<String, String>>) container.callMethod(rubyReceiver, "reduce", key, values, params);
        }
    }
}
