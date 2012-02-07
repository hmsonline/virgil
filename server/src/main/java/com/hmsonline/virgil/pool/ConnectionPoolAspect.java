package com.hmsonline.virgil.pool;

import org.apache.cassandra.thrift.Cassandra;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class ConnectionPoolAspect {
    private static Logger logger = LoggerFactory.getLogger(ConnectionPoolAspect.class);

    public Object delegate(ProceedingJoinPoint thisJoinPoint) throws Throwable {
        ConnectionPoolClient client = (ConnectionPoolClient) thisJoinPoint.getTarget();
        Cassandra.Iface connection = ConnectionPool.getConnection(client);
        client.setConnection(connection);
        try {
            return thisJoinPoint.proceed(thisJoinPoint.getArgs());
        } finally {
            ConnectionPool.release(client, connection);
        }
    }

    @Pointcut("execution(@PooledConnection * com.hmsonline.virgil..*.*(..))")
    public void methodAnnotatedWithPooledConnection() {}

    @Around("methodAnnotatedWithPooledConnection()")
    public Object handleStorage(ProceedingJoinPoint thisJoinPoint) throws Throwable {
        logger.debug("AOP:STORAGE connection for [" + thisJoinPoint.getSignature() + "]");
        return delegate(thisJoinPoint);
    }
}
