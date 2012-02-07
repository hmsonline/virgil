package com.hmsonline.virgil.pool;

public class EmptyConnectionPoolException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public EmptyConnectionPoolException(String message){
        super(message);
    }

    public EmptyConnectionPoolException(String message, Exception e){
        super(message, e);
    }
}
