package com.hmsonline.virgil.config;

import javax.validation.constraints.NotNull;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.hibernate.validator.constraints.NotEmpty;

import com.yammer.dropwizard.config.Configuration;

public class VirgilConfiguration extends Configuration {
    public final static String CASSANDRA_HOST_PROPERTY = "virgil.cassandra_host";
    public final static String CASSANDRA_PORT_PROPERTY = "virgil.cassandra_port";
    public final static String CASSANDRA_EMBEDDED = "virgil.embedded";

    @NotEmpty
    @NotNull
    private String solrHost;

    @NotEmpty
    @NotNull
    private String cassandraYaml;

    private boolean enableIndexing;

    public String getSolrHost() {
        return solrHost;
    }

    public String getCassandraYaml() {
        return cassandraYaml;
    }

    public boolean isIndexingEnabled() {
        return enableIndexing;
    }

    public ConsistencyLevel getConsistencyLevel(String consistencyLevel) {
        // Defaulting consistency level to ALL
        if (consistencyLevel == null)
            return ConsistencyLevel.ALL;
        else
            return ConsistencyLevel.valueOf(consistencyLevel);
    }

    public static boolean isEmbedded() {
        if (System.getProperty(VirgilConfiguration.CASSANDRA_EMBEDDED) == null)
            return true;
        else
            return (System.getProperty(VirgilConfiguration.CASSANDRA_EMBEDDED).equals("1"));
    }

    public static String getHost() {
        return System.getProperty(VirgilConfiguration.CASSANDRA_HOST_PROPERTY);
    }

    public static int getPort() {
        return Integer.parseInt(System.getProperty(VirgilConfiguration.CASSANDRA_PORT_PROPERTY));
    }
}