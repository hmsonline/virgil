package org.apache.virgil.aop;

import java.util.List;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.virgil.triggers.DistributedCommitLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect CassandraServerAspect {
    private static Logger logger = LoggerFactory.getLogger(CassandraServerAspect.class);

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
                    logger.debug("Mutation for [" + rowMutation.getTable() + "] with consistencyLevel ["
                            + consistencyLevel + "]");
                    if (!rowMutation.getTable().equals(DistributedCommitLog.KEYSPACE)) {
                        DistributedCommitLog.writeMutation(consistencyLevel, rowMutation);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
