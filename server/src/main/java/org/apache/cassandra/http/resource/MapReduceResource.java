package org.apache.cassandra.http.resource;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.cassandra.http.CassandraStorage;
import org.apache.cassandra.http.mapreduce.JobSpawner;
import org.apache.cassandra.http.ws.VirgilService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/job/")
public class MapReduceResource {
	private static Logger logger = LoggerFactory.getLogger(MapReduceResource.class);
	private VirgilService virgilService = null;

	public MapReduceResource(VirgilService virgilService) {
		this.virgilService = virgilService;
	}

	// ================================================================================================================
	// Map Reduce
	// ================================================================================================================

	@POST
	@Path("/")
	@Produces({ "text/plain" })
	public void mapReduce(
            @QueryParam("params") String params,
	        @QueryParam("jobName") String jobName, @QueryParam("inputKeyspace") String inputKeyspace,
			@QueryParam("inputColumnFamily") String inputColumnFamily,
			@QueryParam("outputKeyspace") String outputKeyspace,
			@QueryParam("outputColumnFamily") String outputColumnFamily, @QueryParam("remote") boolean remote,
			String source) throws Exception {
		if (inputKeyspace == null)
			throw new RuntimeException("Must supply inputKeyspace.");
		if (inputColumnFamily == null)
			throw new RuntimeException("Must supply inputColumnFamily.");
		if (outputKeyspace == null)
			throw new RuntimeException("Must supply outputKeyspace.");
		if (outputColumnFamily == null)
			throw new RuntimeException("Must supply outputColumnFamily.");

		if (logger.isDebugEnabled()) {
			logger.debug("Launching job [" + jobName + "]");
			logger.debug("  --> Input  : Keyspace [" + inputKeyspace + "], ColumnFamily [" + inputColumnFamily + "]");
			logger.debug("  <-- Output : Keyspace [" + outputKeyspace + "], ColumnFamily [" + outputColumnFamily + "]");
		}

		if (remote) {
			
		} else {
			JobSpawner.spawnLocal(jobName, this.getCassandraStorage().getHost(), 
					this.getCassandraStorage().getPort(), inputKeyspace,
					inputColumnFamily, outputKeyspace, outputColumnFamily, source, params);
		}
	}
	
	public CassandraStorage getCassandraStorage(){
		return this.virgilService.getStorage();
	}
}
