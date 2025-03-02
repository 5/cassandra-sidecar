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

package org.apache.cassandra.sidecar.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SidecarSchema} setup.
 */
@ExtendWith(VertxExtension.class)
public class SidecarSchemaTest
{
    private static final Logger logger = LoggerFactory.getLogger(SidecarSchemaTest.class);
    public static final String DEFAULT_SIDECAR_SCHEMA_KEYSPACE_NAME = "sidecar_internal";
    private static final List<String> interceptedExecStmts = new ArrayList<>();
    private static final List<String> interceptedPrepStmts = new ArrayList<>();

    private Vertx vertx;
    private SidecarSchema sidecarSchema;
    Server server;

    @BeforeEach
    void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(Modules.override(new TestModule())
                                                                     .with(new SidecarSchemaTestModule())));
        this.vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        sidecarSchema = injector.getInstance(SidecarSchema.class);

        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        interceptedExecStmts.clear();
        interceptedPrepStmts.clear();
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onComplete(result -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @Test
    void testSchemaInitOnStartup(VertxTestContext context)
    {
        sidecarSchema.maybeStartSidecarSchemaInitializer();
        context.verify(() -> {
            int maxWaitTime = 20; // about 10 seconds
            while (interceptedPrepStmts.size() < 10
                   || interceptedExecStmts.size() < 3
                   || !sidecarSchema.isInitialized())
            {
                if (maxWaitTime-- <= 0)
                {
                    context.failNow("test timeout");
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }

            assertThat(interceptedExecStmts.size()).isEqualTo(6);
            assertThat(interceptedExecStmts.get(0)).as("Create keyspace should be executed the first")
                                                   .contains("CREATE KEYSPACE IF NOT EXISTS sidecar_internal");
            assertThat(interceptedExecStmts).as("Create table should be executed for job table")
                                            .anyMatch(stmt -> stmt.contains("CREATE TABLE IF NOT EXISTS sidecar_internal.restore_job_v4"));
            assertThat(interceptedExecStmts).as("Create table should be executed for slice table")
                                            .anyMatch(stmt -> stmt.contains("CREATE TABLE IF NOT EXISTS sidecar_internal.restore_slice_v3"));
            assertThat(interceptedExecStmts).as("Create table should be executed for range table")
                                            .anyMatch(stmt -> stmt.contains("CREATE TABLE IF NOT EXISTS sidecar_internal.restore_range_v1"));
            assertThat(interceptedExecStmts).as("Create table should be executed for role_permissions_v1 table")
                                            .anyMatch(stmt -> stmt.contains("CREATE TABLE IF NOT EXISTS sidecar_internal.role_permissions_v1"));

            List<String> expectedPrepStatements = Arrays.asList(
            "INSERT INTO sidecar_internal.restore_job_v4 (  created_at,  job_id,  keyspace_name,  table_name,  " +
            "job_agent,  status,  blob_secrets,  import_options,  consistency_level,  local_datacenter,  expire_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",

            "INSERT INTO sidecar_internal.restore_job_v4 (  created_at,  job_id,  blob_secrets) VALUES (?, ? ,?)",

            "INSERT INTO sidecar_internal.restore_job_v4 (  created_at,  job_id,  status) VALUES (?, ?, ?)",

            "INSERT INTO sidecar_internal.restore_job_v4 (  created_at,  job_id,  job_agent) VALUES (?, ?, ?)",

            "INSERT INTO sidecar_internal.restore_job_v4 (  created_at,  job_id,  expire_at) VALUES (?, ?, ?)",

            "INSERT INTO sidecar_internal.restore_job_v4 (  created_at,  job_id,  slice_count) VALUES (?, ?, ?)",

            "SELECT created_at, job_id, keyspace_name, table_name, job_agent, status, blob_secrets, import_options, " +
            "consistency_level, local_datacenter, expire_at, slice_count FROM sidecar_internal.restore_job_v4 WHERE created_at = ? AND job_id = ?",

            "SELECT created_at, job_id, keyspace_name, table_name, job_agent, status, blob_secrets, import_options, " +
            "consistency_level, local_datacenter, expire_at, slice_count FROM sidecar_internal.restore_job_v4 WHERE created_at = ?",

            "INSERT INTO sidecar_internal.restore_slice_v3 (  job_id,  bucket_id,  slice_id,  bucket,  key,  " +
            "checksum,  start_token,  end_token,  compressed_size,  uncompressed_size) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",

            "SELECT job_id, bucket_id, slice_id, bucket, key, checksum, start_token, end_token, compressed_size, " +
            "uncompressed_size FROM sidecar_internal.restore_slice_v3 " +
            "WHERE job_id = ? AND bucket_id = ? AND end_token > ? AND start_token < ? ALLOW FILTERING",

            "UPDATE sidecar_internal.restore_range_v1 SET slice_id = ?, slice_bucket = ?, slice_key = ?, status_by_replica = status_by_replica + ? " +
            "WHERE job_id = ? AND bucket_id = ? AND start_token = ? AND end_token = ?",

            "SELECT job_id, bucket_id, slice_id, slice_bucket, slice_key, start_token, end_token, status_by_replica " +
            "FROM sidecar_internal.restore_range_v1 WHERE job_id = ? AND bucket_id = ? ALLOW FILTERING",

            "UPDATE sidecar_internal.restore_range_v1 SET status_by_replica = status_by_replica + ? " +
            "WHERE job_id = ? AND bucket_id = ? AND start_token = ? AND end_token = ?",

            "INSERT INTO sidecar_internal.sidecar_lease_v1 (name,owner) " +
            "VALUES ('cluster_lease_holder',?) IF NOT EXISTS USING TTL 120",

            "UPDATE sidecar_internal.sidecar_lease_v1 USING TTL 120 SET owner = ? " +
            "WHERE name = 'cluster_lease_holder' IF owner = ?",

            "SELECT * FROM sidecar_internal.role_permissions_v1",

            "SELECT is_superuser FROM system_auth.roles WHERE role = ?",

            "SELECT * FROM system_auth.roles",

            "SELECT * FROM system_auth.role_permissions"
            );

            assertThat(interceptedPrepStmts).as("Intercepted statements match expected statements")
                                            .containsExactlyInAnyOrderElementsOf(expectedPrepStatements);

            assertThat(sidecarSchema.isInitialized()).as("Schema is successfully initialized").isTrue();
            context.completeNow();
        });
    }

    /**
     * Test module override for {@link SidecarSchemaTest}
     */
    public static class SidecarSchemaTestModule extends AbstractModule
    {
        public final boolean intercept;

        public SidecarSchemaTestModule()
        {
            this(true);
        }

        public SidecarSchemaTestModule(boolean intercept)
        {
            this.intercept = intercept;
        }

        @Provides
        @Singleton
        public CQLSessionProvider cqlSessionProvider()
        {
            CQLSessionProvider cqlSession = mock(CQLSessionProvider.class);
            Session session = mock(Session.class, RETURNS_DEEP_STUBS);
            KeyspaceMetadata ks = mock(KeyspaceMetadata.class);
            when(ks.getTable(anyString())).thenReturn(null);
            when(session.getCluster()
                        .getMetadata()
                        .getKeyspace(anyString())).thenAnswer((Answer<KeyspaceMetadata>) invocation -> {
                if (DEFAULT_SIDECAR_SCHEMA_KEYSPACE_NAME.equals(invocation.getArgument(0)))
                {
                    return null;
                }
                return ks;
            });
            when(session.execute(any(String.class))).then(invocation -> {
                if (intercept)
                {
                    interceptedExecStmts.add(invocation.getArgument(0));
                }
                ResultSet rs = mock(ResultSet.class);
                ExecutionInfo ei = mock(ExecutionInfo.class);
                when(ei.isSchemaInAgreement()).thenReturn(true);
                when(rs.getExecutionInfo()).thenReturn(ei);
                return rs;
            });
            when(session.prepare(any(String.class))).then(invocation -> {
                if (intercept)
                {
                    interceptedPrepStmts.add(invocation.getArgument(0));
                }
                PreparedStatement ps = mock(PreparedStatement.class);
                BoundStatement stmt = mock(BoundStatement.class);
                when(ps.bind(any(Object[].class))).thenReturn(stmt);
                return ps;
            });
            when(cqlSession.get()).thenReturn(session);
            when(cqlSession.getIfConnected()).thenReturn(session);
            return cqlSession;
        }

        @Provides
        @Singleton
        public InstancesMetadata instancesMetadata()
        {
            InstanceMetadata instanceMeta = mock(InstanceMetadata.class);
            when(instanceMeta.stagingDir()).thenReturn("/tmp/staging"); // not an actual file
            InstancesMetadata instancesMetadata = mock(InstancesMetadata.class);
            when(instancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMeta));
            when(instancesMetadata.instanceFromHost(any())).thenReturn(instanceMeta);
            when(instancesMetadata.instanceFromId(anyInt())).thenReturn(instanceMeta);
            return instancesMetadata;
        }

        @Provides
        @Singleton
        public ClusterLease clusterLease()
        {
            return new ClusterLease(ClusterLease.Ownership.CLAIMED);
        }
    }
}
