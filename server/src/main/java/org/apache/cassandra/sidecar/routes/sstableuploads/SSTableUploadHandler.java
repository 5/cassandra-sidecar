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

package org.apache.cassandra.sidecar.routes.sstableuploads;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.KeyspaceMetadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.SSTableUploadResponse;
import org.apache.cassandra.sidecar.concurrent.ConcurrencyLimiter;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceResourceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.UploadSSTableMetrics;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.AccessProtected;
import org.apache.cassandra.sidecar.routes.data.SSTableUploadRequestParam;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.DigestVerifier;
import org.apache.cassandra.sidecar.utils.DigestVerifierFactory;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.MetadataUtils;
import org.apache.cassandra.sidecar.utils.SSTableUploader;
import org.apache.cassandra.sidecar.utils.SSTableUploadsPathBuilder;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;
import static org.apache.cassandra.sidecar.utils.MetricUtils.parseSSTableComponent;

/**
 * Handler for managing uploaded SSTable components
 */
@Singleton
public class SSTableUploadHandler extends AbstractHandler<SSTableUploadRequestParam> implements AccessProtected
{
    private final FileSystem fs;
    private final SSTableUploadConfiguration configuration;
    private final SSTableUploader uploader;
    private final SSTableUploadsPathBuilder uploadPathBuilder;
    private final ConcurrencyLimiter limiter;
    private final DigestVerifierFactory digestVerifierFactory;

    /**
     * Constructs a handler with the provided params.
     *
     * @param vertx                 the vertx instance
     * @param serviceConfiguration  configuration object holding config details of Sidecar
     * @param metadataFetcher       the interface to retrieve metadata
     * @param uploader              a class that uploads the components
     * @param uploadPathBuilder     a class that provides SSTableUploads directories
     * @param executorPools         executor pools for blocking executions
     * @param validator             a validator instance to validate Cassandra-specific input
     * @param digestVerifierFactory a factory of checksum verifiers
     */
    @Inject
    protected SSTableUploadHandler(Vertx vertx,
                                   ServiceConfiguration serviceConfiguration,
                                   InstanceMetadataFetcher metadataFetcher,
                                   SSTableUploader uploader,
                                   SSTableUploadsPathBuilder uploadPathBuilder,
                                   ExecutorPools executorPools,
                                   CassandraInputValidator validator,
                                   DigestVerifierFactory digestVerifierFactory)
    {
        super(metadataFetcher, executorPools, validator);
        this.fs = vertx.fileSystem();
        this.configuration = serviceConfiguration.sstableUploadConfiguration();
        this.uploader = uploader;
        this.uploadPathBuilder = uploadPathBuilder;
        this.limiter = new ConcurrencyLimiter(configuration::concurrentUploadsLimit);
        this.digestVerifierFactory = digestVerifierFactory;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.UPLOAD_STAGED_SSTABLE.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               SSTableUploadRequestParam request)
    {
        // We pause request here, otherwise data streaming will happen before we have our temporary
        // file ready for streaming, and we will see request has already been read error. Hence, we
        // pause request here and resume it when temporary file has been created and is ready to
        // accept the upload.
        httpRequest.pause();

        InstanceMetrics instanceMetrics = metadataFetcher.instance(host).metrics();
        UploadSSTableMetrics.UploadSSTableComponentMetrics componentMetrics
        = instanceMetrics.uploadSSTable().forComponent(parseSSTableComponent(request.component()));

        long startTimeInNanos = System.nanoTime();
        if (!limiter.tryAcquire())
        {
            String message = String.format("Concurrent upload limit (%d) exceeded", limiter.limit());
            instanceMetrics.uploadSSTable().throttled.metric.update(1);
            context.fail(wrapHttpException(HttpResponseStatus.TOO_MANY_REQUESTS, message));
            return;
        }
        // to make sure that permit is always released
        context.addEndHandler(v -> limiter.releasePermit());

        validateKeyspaceAndTable(host, request)
        .compose(validRequest -> uploadPathBuilder.resolveStagingDirectory(host))
        .compose(uploadDir -> ensureSufficientSpaceAvailable(uploadDir,
                                                             instanceMetrics.resource()))
        .compose(v -> uploadPathBuilder.build(host, request))
        .compose(uploadDirectory -> {
            DigestVerifier digestVerifier = digestVerifierFactory.verifier(httpRequest.headers());
            return uploader.uploadComponent(httpRequest,
                                            uploadDirectory,
                                            request.component(),
                                            digestVerifier,
                                            configuration.filePermissions());
        })
        .compose(fs::props)
        .onSuccess(fileProps -> {
            long serviceTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
            logger.info("Successfully uploaded SSTable component for request={}, remoteAddress={}, " +
                        "instance={}, sizeInBytes={}, serviceTimeMillis={}",
                        request, remoteAddress, host, fileProps.size(), serviceTimeMillis);
            componentMetrics.bytesUploadedRate.metric.mark(fileProps.size());
            instanceMetrics.uploadSSTable().totalBytesUploadedRate.metric.mark(fileProps.size());

            context.json(new SSTableUploadResponse(request.uploadId(), fileProps.size(), serviceTimeMillis));
        })
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected void processFailure(Throwable cause, RoutingContext context, String host, SocketAddress remoteAddress,
                                  SSTableUploadRequestParam request)
    {
        if (cause instanceof IllegalArgumentException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, cause.getMessage(), cause));
        }
        else
        {
            super.processFailure(cause, context, host, remoteAddress, request);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SSTableUploadRequestParam extractParamsOrThrow(RoutingContext context)
    {
        return SSTableUploadRequestParam.from(qualifiedTableName(context, true), context);
    }

    /**
     * Ensure that keyspace and table name are valid in the cluster.
     *
     * @param host    the Cassandra instance host
     * @param request the upload request
     * @return {@link Future} containing a valid {@link SSTableUploadRequestParam request}
     */
    private Future<SSTableUploadRequestParam> validateKeyspaceAndTable(String host,
                                                                       SSTableUploadRequestParam request)
    {
        TaskExecutorPool pool = executorPools.service();
        return pool.executeBlocking(() -> metadataFetcher.delegate(host).metadata())
                   .compose(metadata -> {
                       KeyspaceMetadata keyspaceMetadata = MetadataUtils.keyspace(metadata, request.keyspace());
                       if (keyspaceMetadata == null)
                       {
                           String message = String.format("Invalid keyspace '%s' supplied", request.keyspace());
                           logger.error(message);
                           return Future.failedFuture(wrapHttpException(HttpResponseStatus.BAD_REQUEST, message));
                       }

                       if (MetadataUtils.table(keyspaceMetadata, request.table()) == null)
                       {
                           String message = String.format("Invalid table name '%s' supplied for keyspace '%s'",
                                                          request.table(), request.keyspace());
                           logger.error(message);
                           return Future.failedFuture(wrapHttpException(HttpResponseStatus.BAD_REQUEST, message));
                       }
                       return Future.succeededFuture(request);
                   });
    }

    /**
     * Ensures there is sufficient space available as per configured in the
     * {@link SSTableUploadConfiguration#minimumSpacePercentageRequired()}.
     *
     * @param uploadDirectory the directory where the SSTables are uploaded
     * @return a succeeded future if there is sufficient space available, or failed future otherwise
     */
    private Future<String> ensureSufficientSpaceAvailable(String uploadDirectory,
                                                          InstanceResourceMetrics resourceMetrics)
    {
        float minimumPercentageRequired = configuration.minimumSpacePercentageRequired();
        if (minimumPercentageRequired == 0)
        {
            return Future.succeededFuture(uploadDirectory);
        }
        return fs.fsProps(uploadDirectory)
                 .compose(fsProps -> {
                     // calculate available disk space percentage
                     long totalSpace = fsProps.totalSpace();
                     long usableSpace = fsProps.usableSpace();
                     resourceMetrics.usableStagingSpace.metric.setValue(usableSpace);

                     // using double for higher precision
                     double spacePercentAvailable = (usableSpace > 0L && totalSpace > 0L)
                                                    ? ((double) usableSpace / (double) totalSpace) * 100D
                                                    : 0D;
                     return Future.succeededFuture(spacePercentAvailable);
                 })
                 .compose(availableDiskSpacePercentage -> {
                     if (availableDiskSpacePercentage < minimumPercentageRequired)
                     {
                         logger.warn("Insufficient space available for upload in stagingDir={}, available={}%, " +
                                     "required={}%", uploadDirectory,
                                     availableDiskSpacePercentage, minimumPercentageRequired);
                         return Future.failedFuture(wrapHttpException(HttpResponseStatus.INSUFFICIENT_STORAGE,
                                                                      "Insufficient space available for upload"));
                     }
                     return Future.succeededFuture(uploadDirectory);
                 });
    }
}
