package org.apache.virgil.triggers;

import java.util.List;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class CassandraServerTriggerAspect {
    private static Logger logger = LoggerFactory.getLogger(CassandraServerTriggerAspect.class);

    @AfterReturning("call(* org.apache.cassandra.thrift.CassandraServer.doInsert(..))")
    public void writeToCommitLog(JoinPoint thisJoinPoint) {
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
                        DistributedCommitLog.getLog().writeMutation(consistencyLevel, rowMutation);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
