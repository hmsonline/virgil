/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.http;

import java.io.File;
import java.net.URL;

import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.lifecycle.SingletonResourceProvider;
import org.apache.cxf.service.model.EndpointInfo;
import org.apache.cxf.transport.Destination;
import org.apache.cxf.transport.DestinationFactory;
import org.apache.cxf.transport.http_jetty.JettyHTTPDestination;
import org.apache.cxf.transport.http_jetty.ServerEngine;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.FileResource;


public class ApacheCxfHttpServer implements IHttpServer
{
    JAXRSServerFactoryBean sf = null;

    public ApacheCxfHttpServer(String host, int port, CassandraStorage cassandraStorage) throws Exception
    {
        sf = new JAXRSServerFactoryBean();
        sf.setResourceClasses(CassandraRestService.class);
        sf.setResourceProvider(CassandraRestService.class,
                new SingletonResourceProvider(new CassandraRestService(cassandraStorage)));
        sf.setAddress("http://" + host + ":" + port + "/");
        sf.create();

        // Add static content using this:
        // http://cxf.apache.org/docs/standalone-http-transport.html
        //this.addStaticContent(sf);
    }

    public void addStaticContent(JAXRSServerFactoryBean serviceFactory) throws Exception{
        EndpointInfo ei = new EndpointInfo();
        ei.setAddress(serviceFactory.getAddress());
        Destination destination = serviceFactory.getDestinationFactory().getDestination(ei);
        JettyHTTPDestination jettyDestination = (JettyHTTPDestination) destination;
        ServerEngine engine = jettyDestination.getEngine();
        Handler handler = engine.getServant(new URL(serviceFactory.getAddress()));
        Server server = handler.getServer(); // The Server

        // We have to create a HandlerList structure that includes both a ResourceHandler for the static
        // content as well as the ContextHandlerCollection created by CXF (which we retrieve as serverHandler). 
        Handler serverHandler = server.getHandler();
        HandlerList handlerList = new HandlerList();
        ResourceHandler resourceHandler = new ResourceHandler();
        handlerList.addHandler(resourceHandler);
        handlerList.addHandler(serverHandler);

        // replace the CXF servlet connect collection with the list.
        server.setHandler(handlerList);
        // and tell the handler list that it is alive.
        handlerList.start();

        // setup the resource handler
        File staticContentFile = new File("src/main/webapp/"); // ordinary pathname.
        URL targetURL = new URL("file://" + staticContentFile.getCanonicalPath());
        FileResource fileResource = new FileResource(targetURL);
        resourceHandler.setBaseResource(fileResource);
    }
    
    public void start()
    {
        try
        {
            sf.create();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void stop()
    {

    }
}
