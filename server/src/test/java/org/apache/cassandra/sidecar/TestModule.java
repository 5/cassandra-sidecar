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

package org.apache.cassandra.sidecar;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.SidecarRateLimiter;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.InstancesMetadataImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.MockCassandraFactory;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.config.yaml.AccessControlConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.CdcConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.PeriodicTaskConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.RestoreJobConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SSTableUploadConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SchemaKeyspaceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.TestServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ThrottleConfigurationImpl;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;

import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Provides the basic dependencies for unit tests.
 */
public class TestModule extends AbstractModule
{
    public static final int RESTORE_MAX_CONCURRENCY = 10;

    public TestCassandraAdapterDelegate delegate;

    @Singleton
    @Provides
    public CassandraAdapterDelegate delegate(Vertx vertx)
    {
        this.delegate = new TestCassandraAdapterDelegate(vertx);
        return delegate;
    }

    @Provides
    @Singleton
    public SidecarConfiguration configuration()
    {
        return abstractConfig();
    }

    protected SidecarConfigurationImpl abstractConfig()
    {
        return abstractConfig(null);
    }

    protected SidecarConfigurationImpl abstractConfig(SslConfiguration sslConfiguration)
    {
        return abstractConfig(sslConfiguration, new AccessControlConfigurationImpl());
    }

    protected SidecarConfigurationImpl abstractConfig(SslConfiguration sslConfiguration,
                                                      AccessControlConfiguration accessControlConfiguration)
    {
        ThrottleConfiguration throttleConfiguration = new ThrottleConfigurationImpl(5, SecondBoundConfiguration.parse("5s"));
        SSTableUploadConfiguration uploadConfiguration = new SSTableUploadConfigurationImpl(0F);
        CdcConfiguration cdcConfiguration = new CdcConfigurationImpl(SecondBoundConfiguration.parse("1s"));
        SchemaKeyspaceConfiguration schemaKeyspaceConfiguration =
        SchemaKeyspaceConfigurationImpl.builder()
                                       .isEnabled(true)
                                       .keyspace("sidecar_internal")
                                       .replicationFactor(1)
                                       .replicationStrategy("SimpleStrategy")
                                       .build();
        ServiceConfiguration serviceConfiguration =
        TestServiceConfiguration.builder()
                                .throttleConfiguration(throttleConfiguration)
                                .schemaKeyspaceConfiguration(schemaKeyspaceConfiguration)
                                .sstableUploadConfiguration(uploadConfiguration)
                                .cdcConfiguration(cdcConfiguration)
                                .build();
        RestoreJobConfiguration restoreJobConfiguration =
        RestoreJobConfigurationImpl.builder()
                                   .restoreJobTablesTtl(SecondBoundConfiguration.parse((TimeUnit.DAYS.toSeconds(14) + 1) + "s"))
                                   .processMaxConcurrency(RESTORE_MAX_CONCURRENCY)
                                   .slowTaskThreshold(SecondBoundConfiguration.parse("10s"))
                                   .slowTaskReportDelay(SecondBoundConfiguration.parse("2m"))
                                   .build();
        PeriodicTaskConfiguration healthCheckConfiguration
        = new PeriodicTaskConfigurationImpl(true,
                                            MillisecondBoundConfiguration.parse("200ms"),
                                            MillisecondBoundConfiguration.parse("1s"));
        return SidecarConfigurationImpl.builder()
                                       .serviceConfiguration(serviceConfiguration)
                                       .sslConfiguration(sslConfiguration)
                                       .accessControlConfiguration(accessControlConfiguration)
                                       .restoreJobConfiguration(restoreJobConfiguration)
                                       .healthCheckConfiguration(healthCheckConfiguration)
                                       .build();
    }

    @Provides
    @Singleton
    public InstancesMetadata instancesMetadata(DnsResolver dnsResolver, CassandraAdapterDelegate delegate)
    {
        return new InstancesMetadataImpl(instancesMetas((TestCassandraAdapterDelegate) delegate), dnsResolver);
    }

    @Provides
    @Singleton
    @Named("IngressFileRateLimiter")
    public SidecarRateLimiter ingressFileRateLimiter(SidecarConfiguration sidecarConfiguration)
    {
        return SidecarRateLimiter.create(sidecarConfiguration.serviceConfiguration()
                                                             .trafficShapingConfiguration()
                                                             .inboundGlobalFileBandwidthBytesPerSecond());
    }

    public List<InstanceMetadata> instancesMetas(TestCassandraAdapterDelegate delegate)
    {
        InstanceMetadata instance1 = mockInstance(delegate,
                                                  "localhost",
                                                  1,
                                                  "src/test/resources/instance1/data",
                                                  "src/test/resources/instance1/sstable-staging",
                                                  "src/test/resources/instance1",
                                                  true);
        InstanceMetadata instance2 = mockInstance(delegate,
                                                  "localhost2",
                                                  2,
                                                  "src/test/resources/instance2/data",
                                                  "src/test/resources/instance2/sstable-staging",
                                                  "src/test/resources/instance2",
                                                  false);
        InstanceMetadata instance3 = mockInstance(delegate,
                                                  "localhost3",
                                                  3,
                                                  "src/test/resources/instance3/data",
                                                  "src/test/resources/instance3/sstable-staging",
                                                  "src/test/resources/instance3",
                                                  true);
        final List<InstanceMetadata> instanceMetas = new ArrayList<>();
        instanceMetas.add(instance1);
        instanceMetas.add(instance2);
        instanceMetas.add(instance3);
        return instanceMetas;
    }

    private InstanceMetadata mockInstance(TestCassandraAdapterDelegate delegate,
                                          String host, int id, String dataDir, String stagingDir, String storageDir, boolean isUp)
    {
        StorageOperations mockStorageOperations = mock(StorageOperations.class);
        when(mockStorageOperations.dataFileLocations()).thenReturn(List.of(dataDir));
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(metadata.getKeyspace(any())).thenReturn(keyspaceMetadata);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(keyspaceMetadata.getTable(any())).thenReturn(tableMetadata);
        delegate.setMetadata(metadata);
        delegate.setStorageOperations(mockStorageOperations);
        if (isUp)
        {
            delegate.setNodeSettings(NodeSettings.builder()
                                                 .releaseVersion("testVersion")
                                                 .partitioner("testPartitioner")
                                                 .sidecarVersion("testSidecar")
                                                 .datacenter("testDC")
                                                 .rpcAddress(InetAddress.getLoopbackAddress())
                                                 .rpcPort(6475)
                                                 .tokens(Collections.singleton("testToken"))
                                                 .build());
        }
        delegate.setIsNativeUp(isUp);
        return InstanceMetadataImpl.builder()
                                   .id(id)
                                   .host(host)
                                   .port(6475)
                                   .stagingDir(stagingDir)
                                   .storageDir(storageDir)
                                   .dataDirs(List.of(dataDir))
                                   .metricRegistry(registry(id))
                                   .delegate(delegate)
                                   .build();
    }

    /**
     * The Mock factory is used for testing purposes, enabling us to test all failures and possible results
     *
     * @return the {@link CassandraVersionProvider}
     */
    @Provides
    @Singleton
    public CassandraVersionProvider cassandraVersionProvider()
    {
        CassandraVersionProvider.Builder builder = new CassandraVersionProvider.Builder();
        builder.add(new MockCassandraFactory());
        return builder.build();
    }
}
