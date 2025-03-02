/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.routes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.adapters.base.exception.OperationUnavailableException;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.exceptions.JmxAuthenticationException;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.NoSuchCassandraInstanceException;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * An abstract {@link Handler Handler&lt;RoutingContext&gt;} that provides common functionality for handler
 * implementations.
 *
 * @param <T> The type of the request data object
 */
public abstract class AbstractHandler<T> implements Handler<RoutingContext>
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected static final String INSTANCE_ID = "instanceId";
    protected static final String KEYSPACE_PATH_PARAM = "keyspace";
    protected static final String TABLE_PATH_PARAM = "table";

    protected final InstanceMetadataFetcher metadataFetcher;
    protected final ExecutorPools executorPools;
    protected final CassandraInputValidator validator;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    protected AbstractHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools,
                              CassandraInputValidator validator)
    {
        this.metadataFetcher = metadataFetcher;
        this.executorPools = executorPools;
        this.validator = validator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(RoutingContext context)
    {
        HttpServerRequest request = context.request();
        String host = host(context);
        SocketAddress remoteAddress = request.remoteAddress();
        T requestParams = null;
        try
        {
            requestParams = extractParamsOrThrow(context);
            logger.debug("{} received request={}, remoteAddress={}, instance={}",
                         this.getClass().getSimpleName(), requestParams, remoteAddress, host);
            handleInternal(context, request, host, remoteAddress, requestParams);
        }
        catch (Exception exception)
        {
            processFailure(exception, context, host, remoteAddress, requestParams);
        }
    }

    /**
     * Extracts the request object from the {@code context}.
     *
     * @param context the request context
     * @return the request object built from the {@code context}
     */
    protected abstract T extractParamsOrThrow(RoutingContext context);

    /**
     * Handles the request with the parameters for this request.
     *
     * @param context       the request context
     * @param httpRequest   the {@link HttpServerRequest} object
     * @param host          the host where this request is intended for
     * @param remoteAddress the address where the request originates
     * @param request       the request object
     */
    protected abstract void handleInternal(RoutingContext context,
                                           HttpServerRequest httpRequest,
                                           @NotNull String host,
                                           SocketAddress remoteAddress,
                                           T request);

    /**
     * Returns the host from the path if the requests contains the {@code /instance/} path parameter,
     * otherwise it returns the host parsed from the request.
     *
     * @param context the routing context
     * @return the host for the routing context
     * @throws HttpException when the {@code /instance/} path parameter is {@code null}
     */
    @NotNull
    public String host(RoutingContext context)
    {
        if (context.request().params().contains(INSTANCE_ID))
        {
            String instanceIdParam = context.request().getParam(INSTANCE_ID);
            if (instanceIdParam == null)
            {
                throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                        "InstanceId query parameter must be provided");
            }

            try
            {
                int instanceId = Integer.parseInt(instanceIdParam);
                return metadataFetcher.instance(instanceId).host();
            }
            catch (NumberFormatException ex)
            {
                throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                        "InstanceId query parameter must be a valid integer");
            }
            catch (NoSuchCassandraInstanceException | IllegalStateException ex)
            {
                throw new HttpException(HttpResponseStatus.NOT_FOUND.code(), ex.getMessage());
            }
        }
        else
        {
            try
            {
                return extractHostAddressWithoutPort(context.request());
            }
            catch (IllegalArgumentException ex)
            {
                throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(), ex.getMessage());
            }
        }
    }

    /**
     * Processes the failure while handling the request.
     *
     * @param cause         the cause
     * @param context       the routing context
     * @param host          the host where this request is intended for
     * @param remoteAddress the address where the request originates
     * @param request       the request object
     */
    protected void processFailure(Throwable cause, RoutingContext context, String host, SocketAddress remoteAddress,
                                  T request)
    {
        HttpException httpException = determineHttpException(cause);

        if (HttpStatusClass.CLIENT_ERROR.contains(httpException.getStatusCode()))
        {
            logger.warn("{} request failed due to client error. request={}, remoteAddress={}, instance={}",
                        this.getClass().getSimpleName(), request, remoteAddress, host, cause);
        }
        else if (HttpStatusClass.SERVER_ERROR.contains(httpException.getStatusCode()))
        {
            logger.error("{} request failed due to server error. request={}, remoteAddress={}, instance={}",
                         this.getClass().getSimpleName(), request, remoteAddress, host, cause);
        }

        context.fail(httpException);
    }

    protected HttpException determineHttpException(Throwable cause)
    {
        if (cause instanceof HttpException)
        {
            return (HttpException) cause;
        }

        if (cause instanceof JmxAuthenticationException)
        {
            return wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE, cause);
        }

        if (cause instanceof OperationUnavailableException)
        {
            return wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE, cause.getMessage(), cause);
        }

        if (cause instanceof NoSuchCassandraInstanceException)
        {
            return wrapHttpException(HttpResponseStatus.MISDIRECTED_REQUEST, cause.getMessage(), cause);
        }

        if (cause instanceof IllegalArgumentException)
        {
            return wrapHttpException(HttpResponseStatus.BAD_REQUEST, cause.getMessage(), cause);
        }

        if (cause instanceof IllegalStateException)
        {
            return wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, cause.getMessage(), cause);
        }

        return wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, cause);
    }

    /**
     * Returns the validated {@link QualifiedTableName} from the context, where the both keyspace and table name
     * are required.
     *
     * @param context the event to handle
     * @return the validated {@link QualifiedTableName} from the context
     */
    protected QualifiedTableName qualifiedTableName(RoutingContext context)
    {
        return qualifiedTableName(context, true);
    }

    /**
     * Returns the validated {@link QualifiedTableName} from the context.
     *
     * @param context  the event to handle
     * @param required whether keyspace and table name are required
     * @return the validated {@link QualifiedTableName} from the context
     */
    protected QualifiedTableName qualifiedTableName(RoutingContext context, boolean required)
    {
        return new QualifiedTableName(keyspace(context, required),
                                      tableName(context, required));
    }

    /**
     * Returns the validated keyspace name from the context.
     *
     * @param context  the event to handle
     * @param required whether the keyspace is required
     * @return the validated keyspace name from the context
     */
    protected Name keyspace(RoutingContext context, boolean required)
    {
        String keyspace = context.pathParam(KEYSPACE_PATH_PARAM);
        if (required || keyspace != null)
        {
            return validator.validateKeyspaceName(keyspace);
        }
        return null;
    }

    /**
     * Returns the validated table name from the context.
     *
     * @param context  the event to handle
     * @param required whether the table name is required
     * @return the validated table name from the context
     */
    private Name tableName(RoutingContext context, boolean required)
    {
        String tableName = context.pathParam(TABLE_PATH_PARAM);
        if (required || tableName != null)
        {
            return validator.validateTableName(tableName);
        }
        return null;
    }

    /**
     * Given a combined host address like 127.0.0.1:9042 or [2001:db8:0:0:0:ff00:42:8329]:9042, this method
     * removes port information and returns 127.0.0.1 or 2001:db8:0:0:0:ff00:42:8329.
     *
     * @param request http server request
     * @return host address without port information
     * @throws IllegalArgumentException thrown when host header is missing in the request
     */
    @NotNull
    public static String extractHostAddressWithoutPort(HttpServerRequest request) throws IllegalArgumentException
    {
        String host = request.host();
        Preconditions.checkArgument(host != null, "Missing 'host' header in the request");

        if (host.contains(":"))
        {
            // just ipv6 host name present without port information
            if (host.split(":").length > 2 && !host.startsWith("["))
            {
                return host;
            }
            String hostWithoutPort = host.substring(0, host.lastIndexOf(':'));
            return hostWithoutPort.startsWith("[")
                   ? hostWithoutPort.substring(1, hostWithoutPort.length() - 1) // remove brackets from ipv6 addresses
                   : hostWithoutPort; // return ipv4 directly
        }
        return host;
    }
}
