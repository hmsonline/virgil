package com.hmsonline.virgil.pool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;

public class ConnectionPoolClient {
    ThreadLocal<Cassandra.Iface> connection = new ThreadLocal<Cassandra.Iface>() {
        @Override
        protected Cassandra.Iface initialValue() {
           throw new RuntimeException("Using connection w/o attaining from the pool.");
        }
    };

    public void setConnection(Cassandra.Iface connection) {
        this.connection.set(connection);
    }

    public Cassandra.Iface getConnection(String keyspace) throws InvalidRequestException, TException{
        Cassandra.Iface connection = this.connection.get();
        if (keyspace != null)
            connection.set_keyspace(keyspace);
        return connection;
    }
    
    
    
}
