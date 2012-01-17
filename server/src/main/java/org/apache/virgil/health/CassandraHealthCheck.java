package org.apache.virgil.health;

import org.apache.virgil.ws.VirgilService;
import org.json.simple.JSONArray;
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
			String host = this.service.getStorage().getHost();
            int port = this.service.getStorage().getPort();
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
