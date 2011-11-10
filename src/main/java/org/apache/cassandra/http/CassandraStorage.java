/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.http;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.http.index.Indexer;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class CassandraStorage 
{
    private static final int MAX_COLUMNS = 1000;
    static public CassandraServer server = new CassandraServer();
    private Indexer indexer = null;

    // TODO: Come back and make indexing AOP
    public CassandraStorage(Indexer indexer){
    	this.indexer = indexer;
    }
    
    /*
     * Set's thread local value on the server.
     */
    public void setKeyspace(String keyspace) throws Exception
    {
        server.set_keyspace(keyspace);
    }

    public void addKeyspace(String keyspace) throws Exception
    {
        // TODO: Take key space in via JSON/XML. (Replace hard-coded values)
        List<CfDef> cfDefList = new ArrayList<CfDef>();
        KsDef ksDef = new KsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
        ksDef.putToStrategy_options("replication_factor", "1");
        server.system_add_keyspace(ksDef);
    }

    public void dropColumnFamily(String columnFamily) throws Exception
    {
        server.system_drop_column_family(columnFamily);
    }

    public void dropKeyspace(String keyspace) throws Exception
    {
        server.system_drop_keyspace(keyspace);
    }

    public void createColumnFamily(String keyspace, String columnFamilyName) throws Exception
    {
        // TODO: Take column family definition in via JSON/XML. (Replace hard-coded values)
        CfDef columnFamily = new CfDef(keyspace, columnFamilyName);
        columnFamily.setKey_validation_class("UTF8Type");
        columnFamily.setComparator_type("UTF8Type");
        columnFamily.setDefault_validation_class("UTF8Type");
        server.system_add_column_family(columnFamily);
    }

    @SuppressWarnings("unchecked")
    public void addColumn(String keyspace, String column_family, String rowkey, String column_name, String value,
            ConsistencyLevel consistency_level, boolean index) throws Exception
    {
        JSONObject json = new JSONObject();
        json.put(column_name, value);
        this.setColumn(keyspace, column_family, rowkey, json, consistency_level, index);
        
        // TODO: Revisit adding a single field because it requires a fetch first.
        if (VirgilConfig.isIndexingEnabled() && index){
        	String doc = this.getSlice(keyspace, column_family, rowkey, consistency_level);
        	JSONObject indexJson = (JSONObject) JSONValue.parse(doc);
        	indexJson.put(column_name, value);
        	indexer.index(column_family, rowkey, indexJson);        	
        }
    }
    
    public void setColumn(String keyspace, String column_family, String key, JSONObject json,
            ConsistencyLevel consistency_level, boolean index) throws Exception
    {
    	this.setColumn(keyspace, column_family, key, json, consistency_level, index, System.currentTimeMillis());
    }
    
    public void setColumn(String keyspace, String column_family, String key, JSONObject json,
            ConsistencyLevel consistency_level, boolean index, long timestamp) throws Exception
    {
        List<Mutation> slice = new ArrayList<Mutation>();
        for (Object field : json.keySet())
        {
            String name = (String) field;
            String value = (String) json.get(name);
            Column c = new Column();
            c.setName(ByteBufferUtil.bytes(name));
            c.setValue(ByteBufferUtil.bytes(value));
            c.setTimestamp(timestamp);

            Mutation m = new Mutation();
            ColumnOrSuperColumn cc = new ColumnOrSuperColumn();
            cc.setColumn(c);
            m.setColumn_or_supercolumn(cc);
            slice.add(m);
        } 
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        Map<String, List<Mutation>> cfMutations = new HashMap<String, List<Mutation>>();
        cfMutations.put(column_family, slice);
        mutationMap.put(ByteBufferUtil.bytes(key), cfMutations);
        server.batch_mutate(mutationMap, consistency_level);
        
        if (VirgilConfig.isIndexingEnabled() && index)
        	indexer.index(column_family, key, json);
    }

    public void deleteColumn(String keyspace, String column_family, String key, String column,
            ConsistencyLevel consistency_level, boolean purgeIndex) throws Exception
    {
        ColumnPath path = new ColumnPath(column_family);
        path.setColumn(ByteBufferUtil.bytes(column));
        server.remove(ByteBufferUtil.bytes(key), path, System.currentTimeMillis(), consistency_level);       

        // TODO: Revisit deleting a single field because it requires a fetch first.
        // Evidently it is impossible to remove just a field from a document in SOLR
        // http://stackoverflow.com/questions/4802620/can-you-delete-a-field-from-a-document-in-solr-index
        if (VirgilConfig.isIndexingEnabled() && purgeIndex){
        	String doc = this.getSlice(keyspace, column_family, key, consistency_level);
        	indexer.delete(column_family, key);
        	JSONObject json = (JSONObject) JSONValue.parse(doc);
        	json.remove(column);
        	indexer.index(column_family, key, json);        	
        }
    }
    
    public long deleteRow(String keyspace, String column_family, String key,
            ConsistencyLevel consistency_level, boolean purgeIndex) throws Exception
    {
    	long deleteTime = System.currentTimeMillis();
        ColumnPath path = new ColumnPath(column_family);
        server.remove(ByteBufferUtil.bytes(key), path, deleteTime, consistency_level);

        // Update Index
        if (VirgilConfig.isIndexingEnabled() && purgeIndex){
        	indexer.delete(column_family, key);
        }
        return deleteTime;
    }

    public String getColumn(String keyspace, String column_family, String key, String column,
            ConsistencyLevel consistency_level) throws Exception
    {
        ColumnPath path = new ColumnPath(column_family);
        path.setColumn(ByteBufferUtil.bytes(column));
        ColumnOrSuperColumn column_result = server.get(ByteBufferUtil.bytes(key), path, consistency_level);
        return JsonMarshaller.marshallColumn(column_result);
    }

    public String getSlice(String keyspace, String column_family, String key, ConsistencyLevel consistency_level)
            throws Exception
    {
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, MAX_COLUMNS);
        predicate.setSlice_range(range);
        ColumnParent parent = new ColumnParent(column_family);
        List<ColumnOrSuperColumn> slice = server.get_slice(ByteBufferUtil.bytes(key), parent, predicate,
                consistency_level);
        System.out.println("Got [" + slice.size() + "] columns");

        if (slice.size() > 0)
            return JsonMarshaller.marshallSlice(slice);
        else
            return null;
    }
}
