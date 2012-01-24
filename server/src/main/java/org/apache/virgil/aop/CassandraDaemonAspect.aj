package org.apache.virgil.aop;

public aspect CassandraDaemonAspect {

    private pointcut mainMethod() :
            execution(public static void main(String[]));

    before() : mainMethod() {
        //throw new RuntimeException("CRASH IT!");
        System.out.println("> " + thisJoinPoint);
    }

    after() : mainMethod() {
        System.out.println("< " + thisJoinPoint);
    }
}
