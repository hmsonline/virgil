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

package com.hmsonline.virgil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.hmsonline.cassandra.triggers.ConfigurationStore;
import com.hmsonline.cassandra.triggers.DistributedCommitLog;
import com.hmsonline.cassandra.triggers.TriggerStore;
import com.hmsonline.virgil.config.VirgilConfiguration;
import com.hmsonline.virgil.index.Indexer;
import com.hmsonline.virgil.pool.ConnectionPool;
import com.hmsonline.virgil.pool.ConnectionPoolClient;
import com.hmsonline.virgil.pool.PooledConnection;

public class CassandraStorage extends ConnectionPoolClient {
    private static final int MAX_COLUMNS = 1000;
    private static final int MAX_ROWS = 20;
    private Indexer indexer = null;
    private VirgilConfiguration config = null;

    // TODO: Come back and make indexing AOP
    public CassandraStorage(VirgilConfiguration config, Indexer indexer) throws Exception {
        this.indexer = indexer;
        // CassandraStorage.server = server;
        this.config = config;
        ConnectionPool.initializePool();
        if (VirgilConfiguration.isEmbedded()) {
            // Force instantiation of the singletons
            ConfigurationStore.getStore().getKeyspace();
            TriggerStore.getStore().getKeyspace();
            DistributedCommitLog.getLog().getKeyspace();
        }
    }

    @PooledConnection
    public JSONArray getKeyspaces() throws InvalidRequestException, TException, UnsupportedEncodingException {
        List<KsDef> keyspaces = getConnection(null).describe_keyspaces();
        return JsonMarshaller.marshallKeyspaces(keyspaces, true);
    }

    @PooledConnection
    public void addKeyspace(String keyspace) throws InvalidRequestException, TException, SchemaDisagreementException {
        // TODO: Take key space in via JSON/XML. (Replace hard-coded values)
        List<CfDef> cfDefList = new ArrayList<CfDef>();
        KsDef ksDef = new KsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
        ksDef.putToStrategy_options("replication_factor", "1");
        getConnection(null).system_add_keyspace(ksDef);
    }

    @PooledConnection
    public void dropColumnFamily(String keyspace, String columnFamily) throws InvalidRequestException, TException,
            SchemaDisagreementException {
        getConnection(keyspace).system_drop_column_family(columnFamily);
    }

    @PooledConnection
    public void dropKeyspace(String keyspace) throws InvalidRequestException, SchemaDisagreementException, TException {
        getConnection(keyspace).system_drop_keyspace(keyspace);
    }

    @PooledConnection
    public void createColumnFamily(String keyspace, String columnFamilyName, JSONArray indexedColumnsJson)
            throws InvalidRequestException, SchemaDisagreementException, TException {
        // TODO: Take column family definition in via JSON/XML. (Replace
        // hard-coded values)
        CfDef columnFamily = new CfDef(keyspace, columnFamilyName);
        columnFamily.setKey_validation_class("UTF8Type");
        columnFamily.setComparator_type("UTF8Type");
        columnFamily.setDefault_validation_class("UTF8Type");

        // add indexes on columns
        if (indexedColumnsJson != null && CollectionUtils.isNotEmpty(indexedColumnsJson)) {
            for (Object indexedColumn : indexedColumnsJson) {
                if (indexedColumn != null) {
                    String indexedColumnStr = indexedColumn.toString();
                    if (StringUtils.isNotBlank(indexedColumnStr)) {
                        List<ColumnDef> columnMetadata = columnFamily.getColumn_metadata();
                        columnMetadata = columnMetadata != null ? columnMetadata : new ArrayList<ColumnDef>();
                        ColumnDef colDef = new ColumnDef();
                        colDef.setName(indexedColumnStr.getBytes());
                        colDef.index_type = IndexType.KEYS;
                        colDef.setIndex_name(keyspace + "_" + columnFamilyName + "_" + indexedColumnStr + "_INDEX");
                        columnMetadata.add(colDef);
                        columnFamily.setColumn_metadata(columnMetadata);
                    }
                }
            }
        }
        getConnection(keyspace).system_add_column_family(columnFamily);
    }

    @SuppressWarnings("unchecked")
    @PooledConnection
    public void addColumn(String keyspace, String column_family, String rowkey, String column_name, String value,
            ConsistencyLevel consistency_level, boolean index) throws InvalidRequestException, UnavailableException,
            TimedOutException, TException, HttpException, IOException {
        JSONObject json = new JSONObject();
        json.put(column_name, value);
        this.setColumn(keyspace, column_family, rowkey, json, consistency_level, index);

        // TODO: Revisit adding a single field because it requires a fetch
        // first.
        if (this.config.isIndexingEnabled() && index) {
            JSONObject indexJson = this.getSlice(keyspace, column_family, rowkey, consistency_level);
            indexJson.put(column_name, value);
            indexer.index(column_family, rowkey, indexJson);
        }
    }

    @PooledConnection
    public void setColumn(String keyspace, String column_family, String key, JSONObject json,
            ConsistencyLevel consistency_level, boolean index) throws InvalidRequestException, UnavailableException,
            TimedOutException, TException, HttpException, IOException {
        this.setColumn(keyspace, column_family, key, json, consistency_level, index, System.currentTimeMillis() * 1000);
    }

    @PooledConnection
    public void setColumn(String keyspace, String column_family, String key, JSONObject json,
            ConsistencyLevel consistency_level, boolean index, long timestamp) throws InvalidRequestException,
            UnavailableException, TimedOutException, TException, HttpException, IOException {
        List<Mutation> slice = new ArrayList<Mutation>();
        for (Object field : json.keySet()) {
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
        getConnection(keyspace).batch_mutate(mutationMap, consistency_level);

        if (config.isIndexingEnabled() && index)
            indexer.index(column_family, key, json);
    }

    @PooledConnection
    public void deleteColumn(String keyspace, String column_family, String key, String column,
            ConsistencyLevel consistency_level, boolean purgeIndex) throws InvalidRequestException,
            UnavailableException, TimedOutException, TException, HttpException, IOException {
        ColumnPath path = new ColumnPath(column_family);
        path.setColumn(ByteBufferUtil.bytes(column));
        getConnection(keyspace).remove(ByteBufferUtil.bytes(key), path, System.currentTimeMillis() * 1000,
                consistency_level);

        // TODO: Revisit deleting a single field because it requires a fetch
        // first.
        // Evidently it is impossible to remove just a field from a document in
        // SOLR
        // http://stackoverflow.com/questions/4802620/can-you-delete-a-field-from-a-document-in-solr-index
        if (config.isIndexingEnabled() && purgeIndex) {
            indexer.delete(column_family, key);
            JSONObject json = this.getSlice(keyspace, column_family, key, consistency_level);
            json.remove(column);
            indexer.index(column_family, key, json);
        }
    }

    @PooledConnection
    public long deleteRow(String keyspace, String column_family, String key, ConsistencyLevel consistency_level,
            boolean purgeIndex) throws InvalidRequestException, UnavailableException, TimedOutException, TException,
            HttpException, IOException {
        long deleteTime = System.currentTimeMillis() * 1000;
        ColumnPath path = new ColumnPath(column_family);
        getConnection(keyspace).remove(ByteBufferUtil.bytes(key), path, deleteTime, consistency_level);

        // Update Index
        if (config.isIndexingEnabled() && purgeIndex) {
            indexer.delete(column_family, key);
        }
        return deleteTime;
    }

    @PooledConnection
    public String getColumn(String keyspace, String columnFamily, String key, String column,
            ConsistencyLevel consistencyLevel) throws InvalidRequestException, NotFoundException, UnavailableException,
            TimedOutException, TException, UnsupportedEncodingException {
        ColumnPath path = new ColumnPath(columnFamily);
        path.setColumn(ByteBufferUtil.bytes(column));
        ColumnOrSuperColumn column_result = getConnection(keyspace).get(ByteBufferUtil.bytes(key), path,
                consistencyLevel);
        return new String(column_result.getColumn().getValue(), "UTF8");
    }

    @PooledConnection
    public JSONArray getRows(String keyspace, String columnFamily, String queryStr, ConsistencyLevel consistencyLevel)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException,
            CharacterCodingException {
      if(StringUtils.isNotBlank(queryStr)) {
        return getRowsWithQuery(keyspace, columnFamily, queryStr, consistencyLevel);
      }
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, MAX_COLUMNS);
        predicate.setSlice_range(range);

        KeyRange keyRange = new KeyRange(MAX_ROWS);
        keyRange.setStart_key(ByteBufferUtil.bytes(""));
        keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        ColumnParent parent = new ColumnParent(columnFamily);
        List<KeySlice> rows = getConnection(keyspace).get_range_slices(parent, predicate, keyRange, consistencyLevel);
        return JsonMarshaller.marshallRows(rows, true);
    }
    
    @PooledConnection
    public JSONArray getRowsWithQuery(String keyspace, String columnFamily, String queryStr, ConsistencyLevel consistencyLevel)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException,
            CharacterCodingException {
        Query query = QueryParser.parse(queryStr);
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, MAX_COLUMNS);
        predicate.setSlice_range(range);

        ColumnParent parent = new ColumnParent(columnFamily);
        
        IndexClause indexClause = new IndexClause();
        indexClause.setCount(MAX_ROWS);
        indexClause.setStart_key(new byte[0]);
        for(String keyName : query.getEqStmt().keySet()) {
          indexClause.addToExpressions(new IndexExpression(ByteBufferUtil.bytes(keyName),
                                                           IndexOperator.EQ, ByteBufferUtil.bytes(query.getEqStmt().get(keyName))));
        }
        
        List<KeySlice> rows = getConnection(keyspace).get_indexed_slices(parent, indexClause, predicate,
                ConsistencyLevel.ALL);
        return JsonMarshaller.marshallRows(rows, true);
    }

    @PooledConnection
    public JSONObject getSlice(String keyspace, String columnFamily, String key, ConsistencyLevel consistencyLevel)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException,
            UnsupportedEncodingException {
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, MAX_COLUMNS);
        predicate.setSlice_range(range);
        ColumnParent parent = new ColumnParent(columnFamily);
        List<ColumnOrSuperColumn> slice = getConnection(keyspace).get_slice(ByteBufferUtil.bytes(key), parent,
                predicate, consistencyLevel);
        if (slice.size() > 0)
            return JsonMarshaller.marshallSlice(slice);
        else
            return null;
    }

}
