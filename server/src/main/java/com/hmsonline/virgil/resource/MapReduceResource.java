package com.hmsonline.virgil.resource;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hmsonline.virgil.CassandraStorage;
import com.hmsonline.virgil.VirgilService;
import com.hmsonline.virgil.config.VirgilConfiguration;
import com.hmsonline.virgil.mapreduce.JobSpawner;

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
    public void mapReduce(@QueryParam("params") String params, @QueryParam("jobName") String jobName,
            @QueryParam("inputKeyspace") String inputKeyspace,
            @QueryParam("inputColumnFamily") String inputColumnFamily,
            @QueryParam("outputKeyspace") String outputKeyspace,
            @QueryParam("outputColumnFamily") String outputColumnFamily,
            @QueryParam("mapEmitFlag") String mapEmitFlag,
            @QueryParam("reduceRawDataFlag") String reduceRawDataFlag,
            String source) throws Throwable {
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

        if (VirgilConfiguration.isEmbedded()) {
            logger.debug("Running in embedded mode.");
            JobSpawner.spawnLocal(jobName, VirgilConfiguration.getHost(), VirgilConfiguration.getPort(), inputKeyspace,
                    inputColumnFamily, outputKeyspace, outputColumnFamily, source, params, mapEmitFlag, reduceRawDataFlag);
        } else {
            logger.debug("Spawning job remotely.");
            JobSpawner.spawnRemote(jobName, VirgilConfiguration.getHost(), VirgilConfiguration.getPort(),
                    inputKeyspace, inputColumnFamily, outputKeyspace, outputColumnFamily, source, params, mapEmitFlag, reduceRawDataFlag);
        }
    }

    public CassandraStorage getCassandraStorage() {
        return this.virgilService.getStorage();
    }
}
