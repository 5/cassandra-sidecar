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

package org.apache.cassandra.sidecar.adapters.base;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Partitioners;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.exceptions.NodeBootstrappingException;
import org.apache.cassandra.sidecar.common.server.exceptions.SnapshotAlreadyExistsException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.cassandra.sidecar.adapters.base.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;

/**
 * An implementation of the {@link StorageOperations} that interfaces with Cassandra 4.0 and later
 */
public class CassandraStorageOperations implements StorageOperations
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStorageOperations.class);
    protected final JmxClient jmxClient;
    protected final RingProvider ringProvider;
    protected final TokenRangeReplicaProvider tokenRangeReplicaProvider;
    private volatile List<String> dataFileLocations;

    /**
     * Creates a new instance with the provided {@link JmxClient} and {@link DnsResolver}
     *
     * @param jmxClient   the JMX client used to communicate with the Cassandra instance
     * @param dnsResolver the DNS resolver used to lookup replicas
     */
    public CassandraStorageOperations(JmxClient jmxClient, DnsResolver dnsResolver)
    {
        this(jmxClient,
             new RingProvider(jmxClient, dnsResolver),
             new TokenRangeReplicaProvider(jmxClient, dnsResolver));
    }

    /**
     * Creates a new instances with the provided {@link JmxClient}, {@link RingProvider}, and
     * {@link TokenRangeReplicaProvider}. This constructor is exposed for extensibility.
     *
     * @param jmxClient                 the JMX client used to communicate with the Cassandra instance
     * @param ringProvider              the ring provider instance
     * @param tokenRangeReplicaProvider the token range replica provider
     */
    public CassandraStorageOperations(JmxClient jmxClient,
                                      RingProvider ringProvider,
                                      TokenRangeReplicaProvider tokenRangeReplicaProvider)
    {
        this.jmxClient = jmxClient;
        this.ringProvider = ringProvider;
        this.tokenRangeReplicaProvider = tokenRangeReplicaProvider;
    }

    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     * It logs a warning when the {@code ttl} option is provided as the option is unsupported.
     *
     * @param tag      the tag given to the snapshot; may not be null or empty
     * @param keyspace the keyspace in the Cassandra database to use for the snapshot
     * @param table    the table in the Cassandra database to use for the snapshot
     * @param options  map of options, for example ttl, skipFlush
     */
    @Override
    public void takeSnapshot(@NotNull String tag, @NotNull String keyspace, @NotNull String table,
                             @Nullable Map<String, String> options)
    {
        if (options != null && options.containsKey("ttl"))
        {
            LOGGER.warn("The ttl option is not supported in Cassandra 4.0");
        }

        takeSnapshotInternal(tag, keyspace, table, options);
    }

    /**
     * Actually performs the take snapshot operation of a multiple column family from different keyspaces.
     * A snapshot name must be specified.
     *
     * @param tag      the tag given to the snapshot; may not be null or empty
     * @param keyspace the keyspace in the Cassandra database to use for the snapshot
     * @param table    the table in the Cassandra database to use for the snapshot
     * @param options  map of options, for example ttl, skipFlush
     */
    protected void takeSnapshotInternal(@NotNull String tag,
                                        @NotNull String keyspace,
                                        @NotNull String table,
                                        @Nullable Map<String, String> options)
    {
        requireNonNull(tag, "snapshot tag must be non-null");
        requireNonNull(keyspace, "keyspace for the  must be non-null");
        requireNonNull(table, "table must be non-null");
        try
        {
            jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                     .takeSnapshot(tag, options, keyspace + "." + table);
        }
        catch (IOException e)
        {
            String errorMessage = e.getMessage();
            if (errorMessage != null)
            {
                if (errorMessage.contains("Snapshot " + tag + " already exists"))
                {
                    throw new SnapshotAlreadyExistsException(e);
                }
                else if (errorMessage.contains("Keyspace " + keyspace + " does not exist"))
                {
                    throw new IllegalArgumentException(e);
                }
                else if (errorMessage.contains("Cannot snapshot until bootstrap completes"))
                {
                    throw new NodeBootstrappingException(e);
                }
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearSnapshot(@NotNull String tag, @NotNull String keyspace, @NotNull String table)
    {
        requireNonNull(tag, "snapshot tag must be non-null");
        requireNonNull(keyspace, "keyspace must be non-null");
        requireNonNull(table, "table must be non-null");
        LOGGER.debug("Table is not supported by Cassandra JMX endpoints. " +
                     "Clearing snapshot with tag={} and keyspace={}; table={} is ignored", tag, keyspace, table);
        String[] keyspaces = { keyspace };
        jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                 .clearSnapshot(tag, keyspaces);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RingResponse ring(@Nullable Name keyspace) throws UnknownHostException
    {
        return ringProvider.ring(keyspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TokenRangeReplicasResponse tokenRangeReplicas(@NotNull Name keyspace, @NotNull String partitioner)
    {
        return tokenRangeReplicaProvider.tokenRangeReplicas(keyspace, Partitioners.from(partitioner));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> dataFileLocations()
    {
        if (dataFileLocations == null)
        {
            // Currently, the data file locations will not mutate while the Cassandra process is alive.
            // We can cache the data file locations in process, when a Cassandra disconnection happens,
            // a new CassandraAdapterDelegate will be constructed and this value will get repopulated
            // once accessed, if there were any changes to the data file locations for the Cassandra
            // process, Sidecar will get the correct list of data directories.
            String[] allDataFileLocations = jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                                                     .getAllDataFileLocations();
            dataFileLocations = Collections.unmodifiableList(Arrays.asList(allDataFileLocations));
        }
        return dataFileLocations;
    }

    @Override
    public void outOfRangeDataCleanup(@NotNull String keyspace, @NotNull String table, int concurrency)
    throws IOException, ExecutionException, InterruptedException
    {
        requireNonNull(keyspace, "keyspace must be non-null");
        requireNonNull(table, "table must be non-null");
        jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                 .forceKeyspaceCleanup(concurrency, keyspace, table);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String operationMode()
    {
        return jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                        .getOperationMode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decommission(boolean force)
    {
        jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                 .decommission(force);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isGossipRunning()
    {
        return jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                        .isGossipRunning();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String clusterName()
    {
        return jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME)
                        .getClusterName();
    }
}
