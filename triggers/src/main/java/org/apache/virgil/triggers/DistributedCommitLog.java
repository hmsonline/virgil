package org.apache.virgil.triggers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedCommitLog extends InternalCassandraClient {
    private static Logger logger = LoggerFactory.getLogger(DistributedCommitLog.class);

    public static final String KEYSPACE = "virgil";
    public static final String COLUMN_FAMILY = "CommitLog";
    public static final int MAX_ROW_SIZE = 10;
    public static final int BATCH_SIZE = 50;
    public static final int IN_FUTURE = 1000 * 60;
    private static boolean initialized = false;
    private static DistributedCommitLog instance = null;

    public static synchronized DistributedCommitLog getLog() {
        if (instance == null)
            instance = new DistributedCommitLog();
        return instance;
    }

    public synchronized void create() throws Exception {
        if (!initialized) {
            try {
                List<CfDef> cfDefList = new ArrayList<CfDef>();
                KsDef ksDef = new KsDef(KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
                ksDef.putToStrategy_options("replication_factor", "1");
                getConnection(null).system_add_keyspace(ksDef);
            } catch (Exception e) {
                logger.debug("Did not create System.CommitLog. (probably already there)");
            }
            try {
                CfDef columnFamily = new CfDef(KEYSPACE, COLUMN_FAMILY);
                columnFamily.setKey_validation_class("UTF8Type");
                columnFamily.setDefault_validation_class("UTF8Type");
                columnFamily.setComparator_type("UTF8Type");

                getConnection(KEYSPACE).system_add_column_family(columnFamily);
                initialized = true;
            } catch (Exception e) {
                logger.debug("Did not create System.CommitLog. (probably already there)");
            }
        }
    }

    public List<LogEntry> writePending(ConsistencyLevel consistencyLevel, RowMutation rowMutation) throws Throwable {
        String keyspace = rowMutation.getTable();
        ByteBuffer rowKey = rowMutation.key();
        List<LogEntry> entries = new ArrayList<LogEntry>();
        for (Integer cfId : rowMutation.getColumnFamilyIds()) {
            ColumnFamily columnFamily = rowMutation.getColumnFamily(cfId);
            LogEntry entry = new LogEntry(keyspace, columnFamily, rowKey, consistencyLevel);
            entries.add(entry);
            writeLogEntry(entry);
        }
        return entries;
    }

    public List<LogEntry> getPending() throws Throwable {
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange(ByteBufferUtil.bytes(""), ByteBufferUtil.bytes(""), false, MAX_ROW_SIZE);
        predicate.setSlice_range(range);

        KeyRange keyRange = new KeyRange(BATCH_SIZE);
        keyRange.setStart_key(ByteBufferUtil.bytes(""));
        keyRange.setEnd_key(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        ColumnParent parent = new ColumnParent(COLUMN_FAMILY);
        List<KeySlice> rows = getConnection(KEYSPACE).get_range_slices(parent, predicate, keyRange,
                ConsistencyLevel.ALL);
        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        for (KeySlice keySlice : rows) {
            if (keySlice.columns.size() > 0) {
                LogEntry logEntry = new LogEntry();
                logEntry.setUuid(ByteBufferUtil.string(keySlice.key));
                for (ColumnOrSuperColumn cc : keySlice.columns) {
                    if (ByteBufferUtil.string(cc.column.name).equals("ks")) {
                        logEntry.setKeyspace(ByteBufferUtil.string(cc.column.value));
                    } else if (ByteBufferUtil.string(cc.column.name).equals("cf")) {
                        logEntry.setColumnFamily(ByteBufferUtil.string(cc.column.value));
                    } else if (ByteBufferUtil.string(cc.column.name).equals("row")) {
                        logEntry.setRowKey(cc.column.value);
                    } else if (ByteBufferUtil.string(cc.column.name).equals("status")) {
                        logEntry.setStatus(LogEntryStatus.valueOf(ByteBufferUtil.string(cc.column.value)));
                    }
                }
                logEntries.add(logEntry);
            }
        }
        return logEntries;
    }

    public void writeLogEntry(LogEntry logEntry) throws Throwable {
        List<Mutation> slice = new ArrayList<Mutation>();
        slice.add(getMutation("ks", logEntry.getKeyspace()));
        slice.add(getMutation("cf", logEntry.getColumnFamily()));
        slice.add(getMutation("row", logEntry.getRowKey()));
        slice.add(getMutation("status", logEntry.getStatus().toString()));
        for (ColumnOperation operation : logEntry.getOperations()) {
            if (operation.isDelete()) {
                slice.add(getMutation(operation.getName(), "DELETE"));
            } else {
                slice.add(getMutation(operation.getName(), "UPDATE"));
            }
        }
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        Map<String, List<Mutation>> cfMutations = new HashMap<String, List<Mutation>>();
        cfMutations.put(COLUMN_FAMILY, slice);

        ByteBuffer rowKey = ByteBufferUtil.bytes(logEntry.getUuid());
        mutationMap.put(rowKey, cfMutations);
        getConnection(KEYSPACE).batch_mutate(mutationMap, logEntry.getConsistencyLevel());
    }

    public void removeLogEntry(LogEntry logEntry) throws Throwable {
        long deleteTime = System.currentTimeMillis() * 1000;
        ColumnPath path = new ColumnPath(COLUMN_FAMILY);
        getConnection(KEYSPACE)
                .remove(ByteBufferUtil.bytes(logEntry.getUuid()), path, deleteTime, ConsistencyLevel.ALL);
    }

    // Utility Methods
    private Mutation getMutation(String name, String value) {
        return getMutation(name, ByteBufferUtil.bytes(value));
    }

    private Mutation getMutation(String name, ByteBuffer value) {
        return getMutation(ByteBufferUtil.bytes(name), value);
    }

    private Mutation getMutation(ByteBuffer name, String value) {
        return getMutation(name, ByteBufferUtil.bytes(value));
    }

    private Mutation getMutation(ByteBuffer name, ByteBuffer value) {
        Column c = new Column();
        c.setName(name);
        c.setValue(value);
        c.setTimestamp(System.currentTimeMillis() * 1000);

        Mutation m = new Mutation();
        ColumnOrSuperColumn cc = new ColumnOrSuperColumn();
        cc.setColumn(c);
        m.setColumn_or_supercolumn(cc);
        return m;
    }
}
