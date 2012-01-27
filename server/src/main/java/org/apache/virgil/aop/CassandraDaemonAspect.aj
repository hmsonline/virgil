package org.apache.virgil.aop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect CassandraDaemonAspect {
    private static Logger logger = LoggerFactory.getLogger(CassandraDaemonAspect.class);

    private pointcut mainMethod() :
            execution(public static void main(String[]));

    before() : mainMethod() {
        //throw new RuntimeException("CRASH IT!");
        logger.debug("> " + thisJoinPoint);
    }

    after() : mainMethod() {
        logger.debug("< " + thisJoinPoint);
    }
}
