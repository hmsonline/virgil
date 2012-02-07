package com.hmsonline.virgil.mapreduce;

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

import com.hmsonline.virgil.mapreduce.RubyInvoker;

public class RubyInvokerTest {

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

    @Test
    public void testMapInvocation() throws Exception {
        String source = getSource();
        ScriptingContainer rubyContainer = new ScriptingContainer(LocalContextScope.CONCURRENT);
        rubyContainer.setLoadPaths(getGemPaths());
        Object rubyReceiver = rubyContainer.runScriptlet(source);

        Map<String, String> columns = new HashMap<String, String>();
        columns.put("collin", "42");
        columns.put("owen", "33");

        RubyArray tuples = RubyInvoker.invokeMap(rubyContainer, rubyReceiver, "rockwall", columns, null);
        assertEquals(2, tuples.size());
    }

    @Test
    public void testReduceInvocation() throws Exception {
        String source = getSource();
        ScriptingContainer rubyContainer = new ScriptingContainer(LocalContextScope.CONCURRENT);
        rubyContainer.setLoadPaths(getGemPaths());
        Object rubyReceiver = rubyContainer.runScriptlet(source);
        String key = "fun";
        List<Object> values = new ArrayList<Object>();
        values.add("1");
        values.add("3");
        values.add("5");
        Map<String, Map<String, String>> results = RubyInvoker.invokeReduce(rubyContainer, rubyReceiver, key, values,
                null);
        assertEquals(1, results.size());
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
