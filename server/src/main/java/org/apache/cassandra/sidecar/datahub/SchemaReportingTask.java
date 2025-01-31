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

package org.apache.cassandra.sidecar.datahub;

import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SchemaReportingConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.ExecuteOnClusterLeaseholderOnly;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link PeriodicTask} that uses provided {@link SchemaReportingConfiguration} to report current cluster schema
 */
@Singleton
public class SchemaReportingTask implements PeriodicTask, ExecuteOnClusterLeaseholderOnly
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaReportingTask.class);
    private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

    @NotNull
    protected final SchemaReportingConfiguration configuration;
    @NotNull
    protected final CQLSessionProvider session;
    @NotNull
    protected final SchemaReporter reporter;

    @Inject
    public SchemaReportingTask(@NotNull SidecarConfiguration configuration,
                               @NotNull CQLSessionProvider session,
                               @NotNull SchemaReporter reporter)
    {
        this.configuration = configuration.schemaReportingConfiguration();
        this.session = session;
        this.reporter = reporter;
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        return configuration.enabled()
                ? ScheduleDecision.EXECUTE
                : ScheduleDecision.SKIP;
    }

    @Override
    public DurationSpec initialDelay()
    {
        MillisecondBoundConfiguration maximum = configuration.initialDelay();

        return new MillisecondBoundConfiguration(RANDOM.nextLong(maximum.quantity()),
                                                 maximum.unit());
    }

    @Override
    public DurationSpec delay()
    {
        return configuration.executeInterval();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            reporter.process(session.get().getCluster());
            promise.complete();
        }
        catch (Throwable throwable)
        {
            LOGGER.error("Failed to convert and report the current schema", throwable);
            promise.fail(throwable);
        }
    }
}
