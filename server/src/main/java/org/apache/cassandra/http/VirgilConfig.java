package org.apache.cassandra.http;

import java.io.InputStream;
import java.util.Map;

import org.apache.cassandra.cli.CliClient;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.yaml.snakeyaml.Yaml;

public class VirgilConfig {
	private static Map<String, Object> config = null;
	public final static String CASSANDRA_HOST_PROPERTY = "virgil.cassandra_host";
	public final static String CASSANDRA_PORT_PROPERTY = "virgil.cassandra_port";

	@SuppressWarnings("unchecked")
	public static Map<String, Object> getConfig() {
		if (VirgilConfig.config == null) {
			final InputStream inputStream = CliClient.class.getClassLoader().getResourceAsStream("virgil.yaml");
			try {
				Yaml yaml = new Yaml();
				VirgilConfig.config = (Map<String, Object>) yaml.load(inputStream);
			} finally {
				FileUtils.closeQuietly(inputStream);
			}
		}
		return VirgilConfig.config;
	}

	public static String getValue(String key) {
		return (String) VirgilConfig.getConfig().get(key);
	}

	public static String getBindHost() {
		return (String) VirgilConfig.getConfig().get("http_bind_host");
	}

	public static int getListenPort() {
		return (Integer) VirgilConfig.getConfig().get("http_listen_port");
	}

	public static boolean isIndexingEnabled() {
		return (Boolean) VirgilConfig.getConfig().get("enable_indexing");
	}

	public static boolean isEmbedded() {
		return (Boolean) VirgilConfig.getConfig().get("embed_cassandra");
	}

	public static String getCassandraHost() {
		return System.getProperty(VirgilConfig.CASSANDRA_HOST_PROPERTY);
	}

	public static Integer getCassandraPort() {
		return new Integer(System.getProperty(VirgilConfig.CASSANDRA_PORT_PROPERTY));
	}

	public static ConsistencyLevel getConsistencyLevel(String consistencyLevel) {
		// Defaulting consistency level to ALL
		if (consistencyLevel == null)
			return ConsistencyLevel.ALL;
		else
			return ConsistencyLevel.valueOf(consistencyLevel);
	}
}
