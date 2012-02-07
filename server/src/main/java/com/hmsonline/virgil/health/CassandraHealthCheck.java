package com.hmsonline.virgil.health;

import org.json.simple.JSONArray;

import com.hmsonline.virgil.VirgilService;
import com.hmsonline.virgil.config.VirgilConfiguration;
import com.yammer.metrics.core.HealthCheck;

public class CassandraHealthCheck extends HealthCheck {
	private VirgilService service;

	public CassandraHealthCheck(VirgilService service) {
		super("Cassandra Check");
		this.service = service;
	}

	@Override
	public Result check() throws Exception {
		Result result = null;
		
		try {
			String host = VirgilConfiguration.getHost();
            int port = VirgilConfiguration.getPort();
			JSONArray keyspaces = this.service.getStorage().getKeyspaces();
			String output = "Connected to [" + host + ":" + port + "] w/ " + keyspaces.size() + " keyspaces.";
			result = Result.healthy(output);
		} catch (Throwable e) {
			result = Result.unhealthy("Unable to connect to cluster: "
					+ e.getMessage());
		}
		return result;
	}
}
