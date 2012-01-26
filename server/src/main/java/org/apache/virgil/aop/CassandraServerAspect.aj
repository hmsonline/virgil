package org.apache.virgil.aop;

import java.util.List;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.virgil.triggers.DistributedCommitLog;

public aspect CassandraServerAspect {

    private pointcut insertMethod() :
            execution(private void doInsert(ConsistencyLevel, List<? extends IMutation>));

    after() : insertMethod() {
        try {
            ConsistencyLevel consistencyLevel = (ConsistencyLevel) thisJoinPoint.getArgs()[0];
            @SuppressWarnings("unchecked")
            List<IMutation> mutations = (List<IMutation>) thisJoinPoint.getArgs()[1];
            for (IMutation mutation : mutations) {
                if (mutation instanceof RowMutation) {
                    RowMutation rowMutation = (RowMutation) mutation;
                    // String key = ByteBufferUtil.string(rowMutation.key());
                    // System.out.println("Mutation for [" + key + "] @ [" +
                    // rowMutation.getTable()
                    // + "] with consistencyLevel [" + consistencyLevel + "]");
                    System.out.println("Mutation for [" + rowMutation.getTable() + "] with consistencyLevel ["
                            + consistencyLevel + "]");
                    if (!rowMutation.getTable().equals(DistributedCommitLog.KEYSPACE)) {
                        DistributedCommitLog.writeMutation(consistencyLevel, rowMutation);
                    }
                    // for (Integer cfId : rowMutation.getColumnFamilyIds()) {
                    // ColumnFamily columnFamily =
                    // rowMutation.getColumnFamily(cfId);
                    // for (IColumn column : columnFamily.getSortedColumns()) {
                    // if (co)
                    // String name = ByteBufferUtil.string(column.name());
                    // String value = ByteBufferUtil.string(column.value());
                    // boolean delete = columnFamily.isMarkedForDelete();
                    // if (delete) {
                    // System.out.println(" -- DELETE -- [" + name + "] => [" +
                    // value + "]");
                    // } else {
                    // System.out.println(" -- SET -- [" + name + "] => [" +
                    // value + "]");
                    // }
                    // }
                    // }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
