package org.apache.cassandra.http;

import java.io.InputStream;
import java.util.Map;

import org.apache.cassandra.cli.CliClient;
import org.apache.cassandra.io.util.FileUtils;
import org.yaml.snakeyaml.Yaml;

public class VirgilConfig {
	private static Map<String, String> config = null;

	@SuppressWarnings("unchecked")
	public static Map<String, String> getConfig() {

		if (VirgilConfig.config == null) {
			final InputStream inputStream = CliClient.class.getClassLoader()
					.getResourceAsStream("virgil.yaml");
			try {
				Yaml yaml = new Yaml();
				VirgilConfig.config = (Map<String, String>) yaml.load(inputStream);
			} finally {
				FileUtils.closeQuietly(inputStream);
			}
		}
		return VirgilConfig.config;

	}
}
