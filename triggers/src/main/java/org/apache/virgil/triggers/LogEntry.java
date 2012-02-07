package org.apache.virgil.triggers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.UUIDGen;

public class LogEntry {
    private String keyspace = null;
    private String columnFamily = null;
    private ConsistencyLevel consistencyLevel = null;
    private List<ColumnOperation> operations = new ArrayList<ColumnOperation>();
    private LogEntryStatus status = null;
    private ByteBuffer rowKey = null;
    private String uuid = null;

    public LogEntry() {
    }

    public LogEntry(String keyspace, ColumnFamily columnFamily, ByteBuffer rowKey, ConsistencyLevel consistencyLevel)
            throws Throwable {
        this.columnFamily = columnFamily.metadata().cfName;
        this.keyspace = keyspace;
        this.rowKey = rowKey;
        for (IColumn column : columnFamily.getSortedColumns()) {
            ColumnOperation operation = new ColumnOperation();
            operation.setName(column.name());
            operation.setDelete(columnFamily.isMarkedForDelete());
            operations.add(operation);
        }
        this.uuid = UUIDGen.getUUID(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())).toString();
        this.status = LogEntryStatus.PREPARING;
        this.consistencyLevel = consistencyLevel;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public List<ColumnOperation> getOperations() {
        return operations;
    }

    public void setOperations(List<ColumnOperation> operations) {
        this.operations = operations;
    }

    public LogEntryStatus getStatus() {
        return status;
    }

    public void setStatus(LogEntryStatus status) {
        this.status = status;
    }

    public ByteBuffer getRowKey() {
        return rowKey;
    }

    public void setRowKey(ByteBuffer rowKey) {
        this.rowKey = rowKey;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

}
