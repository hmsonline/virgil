package org.apache.cassandra.http;

import java.io.InputStream;
import java.util.Map;

import org.apache.cassandra.cli.CliClient;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class VirgilConfig {
	private static Map<String, Object> config = null;
    private static Logger logger = LoggerFactory.getLogger(CassandraRestService.class);

	@SuppressWarnings("unchecked")
	public static Map<String, Object> getConfig() {

		if (VirgilConfig.config == null) {
			final InputStream inputStream = CliClient.class.getClassLoader()
					.getResourceAsStream("virgil.yaml");
			try {
				Yaml yaml = new Yaml();
				VirgilConfig.config = (Map<String, Object>) yaml.load(inputStream);
			} finally {
				FileUtils.closeQuietly(inputStream);
			}
		}
		return VirgilConfig.config;

	}
	
	public static String getValue(String key){
		return (String) VirgilConfig.getConfig().get(key);
	}
	
	public static boolean isIndexingEnabled(){
		return (Boolean)VirgilConfig.getConfig().get("enable_indexing");
	}
}
