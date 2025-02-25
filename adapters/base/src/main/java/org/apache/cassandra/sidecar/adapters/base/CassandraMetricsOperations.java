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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.openmbean.CompositeData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.adapters.base.data.SessionInfo;
import org.apache.cassandra.sidecar.adapters.base.data.StreamState;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStats;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStatsDatabaseAccessor;
import org.apache.cassandra.sidecar.adapters.base.db.ConnectedClientStatsSummary;
import org.apache.cassandra.sidecar.adapters.base.db.schema.ConnectedClientsSchema;
import org.apache.cassandra.sidecar.adapters.base.jmx.MetricsJmxOperations;
import org.apache.cassandra.sidecar.adapters.base.jmx.StreamManagerJmxOperations;
import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.TableStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.ClientConnectionEntry;
import org.apache.cassandra.sidecar.common.response.data.StreamsProgressStats;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.adapters.base.jmx.MetricsJmxOperations.METRICS_OBJ_TYPE_KEYSPACE_TABLE_FORMAT;
import static org.apache.cassandra.sidecar.adapters.base.jmx.StreamManagerJmxOperations.STREAM_MANAGER_OBJ_NAME;

/**
 * Default implementation that pulls methods from the Cassandra Metrics Proxy
 */
public class CassandraMetricsOperations implements MetricsOperations
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMetricsOperations.class);
    private final ConnectedClientStatsDatabaseAccessor dbAccessor;
    protected final JmxClient jmxClient;


    /**
     * Creates a new instance with the provided {@link CQLSessionProvider}
     */
    public CassandraMetricsOperations(JmxClient jmxClient, CQLSessionProvider session)
    {
        this.jmxClient = jmxClient;
        this.dbAccessor = new ConnectedClientStatsDatabaseAccessor(session, new ConnectedClientsSchema());
    }

    /**
     * Represents the types of metrics that are queried
     */
    public enum MetricType
    {
        GAUGE,
        COUNTER
    }

    /**
     * Represents the metrics related to table stats that are supported by the Sidecar
     */
    public enum TableStatsMetrics
    {
        SSTABLE_COUNT("LiveSSTableCount", MetricType.GAUGE),
        DISKSPACE_USED("LiveDiskSpaceUsed", MetricType.COUNTER),
        TOTAL_DISKSPACE_USED("TotalDiskSpaceUsed", MetricType.COUNTER),
        SNAPSHOTS_SIZE("SnapshotsSize", MetricType.GAUGE);

        private final String metricName;
        private final MetricType type;

        TableStatsMetrics(String metricName, MetricType type)
        {
            this.metricName = metricName;
            this.type = type;
        }

        String metricName()
        {
            return metricName;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableStatsResponse tableStats(QualifiedTableName tableName)
    {
        long sstableCount = queryMetric(tableName, TableStatsMetrics.SSTABLE_COUNT);
        long diskSpaceUsed = queryMetric(tableName, TableStatsMetrics.DISKSPACE_USED);
        long totalDiskSpaceUsed = queryMetric(tableName, TableStatsMetrics.TOTAL_DISKSPACE_USED);
        long snapshotsSize = queryMetric(tableName, TableStatsMetrics.SNAPSHOTS_SIZE);

        return new TableStatsResponse(tableName.keyspace(), tableName.tableName(), sstableCount, diskSpaceUsed, totalDiskSpaceUsed, snapshotsSize);
    }

    private long queryMetric(QualifiedTableName tableName, TableStatsMetrics metric)
    {
        String metricObjectType = String.format(METRICS_OBJ_TYPE_KEYSPACE_TABLE_FORMAT, tableName.keyspace(), tableName.tableName(), metric.metricName());
        MetricsJmxOperations queryResult = jmxClient.proxy(MetricsJmxOperations.class, metricObjectType);
        return extractValue(metric, queryResult);
    }

    private long extractValue(TableStatsMetrics metric, MetricsJmxOperations queryResult)
    {
        switch(metric.type)
        {
            case GAUGE: return getValueAsLong(queryResult.getValue());
            case COUNTER: return queryResult.getCount();
            default:
                throw new IllegalArgumentException("Unknown MetricType: " + metric.type);
        }
    }

    private long getValueAsLong(Object value)
    {
        if (value instanceof Integer)
        {
            return ((Integer) value).longValue();
        }
        else if (value instanceof Long)
        {
            return (Long) value;
        }
        else
        {
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectedClientStatsResponse connectedClientStats(boolean summaryOnly)
    {
        if (summaryOnly)
        {
            return connectedClientSummary();
        }
        return connectedClientDetails();
    }

    public ConnectedClientStatsResponse connectedClientDetails()
    {
        List<ClientConnectionEntry> entries = statsToEntries(dbAccessor.stats());
        Map<String, Long> connectionsByUser = entries.stream().collect(Collectors.groupingBy(ClientConnectionEntry::username,
                                                                                             Collectors.counting()));
        long totalConnectedClients = entries.size();
        return new ConnectedClientStatsResponse(entries, totalConnectedClients, connectionsByUser);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamsProgressStats streamsProgressStats()
    {
        Set<CompositeData> streamData = jmxClient.proxy(StreamManagerJmxOperations.class, STREAM_MANAGER_OBJ_NAME)
                                                 .getCurrentStreams();
        return computeStats(streamData.stream().map(StreamState::new));
    }

    private StreamsProgressStats computeStats(Stream<StreamState> streamStates)
    {
        Iterator<SessionInfo> sessions = streamStates.map(StreamState::sessions).flatMap(Collection::stream).iterator();

        long totalFilesToReceive = 0;
        long totalFilesReceived = 0;
        long totalBytesToReceive = 0;
        long totalBytesReceived = 0;

        long totalFilesToSend = 0;
        long totalFilesSent = 0;
        long totalBytesToSend = 0;
        long totalBytesSent = 0;

        while (sessions.hasNext())
        {
            SessionInfo sessionInfo = sessions.next();
            totalBytesToReceive += sessionInfo.totalSizeToReceive();
            totalBytesReceived += sessionInfo.totalSizeReceived();
            totalFilesToReceive += sessionInfo.totalFilesToReceive();
            totalFilesReceived += sessionInfo.totalFilesReceived();
            totalBytesToSend += sessionInfo.totalSizeToSend();
            totalBytesSent += sessionInfo.totalSizeSent();
            totalFilesToSend += sessionInfo.totalFilesToSend();
            totalFilesSent += sessionInfo.totalFilesSent();
        }

        LOGGER.debug("Progress Stats: totalBytesToReceive:{} totalBytesReceived:{} totalBytesToSend:{} totalBytesSent:{}",
                     totalBytesToReceive, totalBytesReceived, totalBytesToSend, totalBytesSent);
        return new StreamsProgressStats(totalFilesToReceive, totalFilesReceived, totalBytesToReceive, totalBytesReceived,
                                        totalFilesToSend, totalFilesSent, totalBytesToSend, totalBytesSent);

    }

    private ConnectedClientStatsResponse connectedClientSummary()
    {
        ConnectedClientStatsSummary summary = dbAccessor.summary();
        return new ConnectedClientStatsResponse(null, summary.totalConnectedClients, summary.connectionsByUser);
    }

    private List<ClientConnectionEntry> statsToEntries(Stream<ConnectedClientStats> stats)
    {
        return stats.map(CassandraMetricsOperations::statToEntry)
                    .collect(Collectors.toList());
    }

    private static @NotNull ClientConnectionEntry statToEntry(ConnectedClientStats stat)
    {
        // Note: We explicitly use constructor params based object creation instead of builder in order to optimize the
        // number of potential objects created for each row of the table queried, specifically since we know this can be large
        return new ClientConnectionEntry(stat.address,
                                         stat.port,
                                         stat.sslEnabled,
                                         stat.sslCipherSuite,
                                         stat.sslProtocol,
                                         stat.protocolVersion,
                                         stat.username,
                                         stat.requestCount,
                                         stat.driverName,
                                         stat.driverVersion,
                                         stat.keyspaceName,
                                         stat.clientOptions,
                                         stat.authenticationMode,
                                         stat.authenticationMetadata);
    }
}
