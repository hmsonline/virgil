//
// Copyright (c) 2012 Health Market Science, Inc.
//
package org.apache.virgil.exception;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.cassandra.thrift.InvalidRequestException;

@Provider
public class KeyspaceExceptionMapper implements ExceptionMapper<InvalidRequestException> {
    public Response toResponse(InvalidRequestException exception) {
        return Response.status(Status.OK).entity(exception.getWhy()).build();
    }
}
