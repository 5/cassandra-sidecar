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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.http.HttpMethod;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SchemaReportingConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration for Schema Reporting
 */
public class SchemaReportingConfigurationImpl extends PeriodicTaskConfigurationImpl implements SchemaReportingConfiguration
{
    protected static final boolean DEFAULT_ENABLED = false;
    protected static final MillisecondBoundConfiguration DEFAULT_DELAY = new MillisecondBoundConfiguration(6L, TimeUnit.HOURS);
    protected static final MillisecondBoundConfiguration DEFAULT_INTERVAL = new MillisecondBoundConfiguration(12L, TimeUnit.HOURS);
    protected static final String DEFAULT_ENDPOINT = null;
    protected static final String DEFAULT_METHOD = HttpMethod.PUT.name();
    protected static final int DEFAULT_RETRIES = 3;

    @JsonProperty(value = "endpoint")
    @Nullable
    protected final String endpoint;
    @JsonProperty(value = "method")
    @Nullable
    protected final String method;
    @JsonProperty(value = "retries")
    protected final int retries;

    /**
     * Constructs an instance of {@link SchemaReportingConfigurationImpl} with default settings
     */
    public SchemaReportingConfigurationImpl()
    {
        this(DEFAULT_ENABLED,
             DEFAULT_DELAY,
             DEFAULT_INTERVAL,
             DEFAULT_ENDPOINT,
             DEFAULT_METHOD,
             DEFAULT_RETRIES);
    }

    /**
     * Constructs an instance of {@link SchemaReportingConfigurationImpl} with custom settings
     *
     * @param enabled whether to report cluster schemata; {@code false} by default
     * @param delay maximum delay before the initial schema report used to prevent the thundering herd problem
     *              (the actual delay will be randomized for each execution, but will not exceed this value); 6 hours by default
     * @param interval exact interval between two consecutive reports of the same schema; 12 hours by default
     * @param endpoint endpoint address for schema reporting; empty by default
     * @param method HTTP verb to use when reporting schemata; {@code PUT} by default
     * @param retries number of times a schema report is retried in case of failure; {@code 3} by default
     */
    public SchemaReportingConfigurationImpl(boolean enabled,
                                            @NotNull MillisecondBoundConfiguration delay,
                                            @NotNull MillisecondBoundConfiguration interval,
                                            @Nullable String endpoint,
                                            @Nullable String method,
                                            int retries)
    {
        super(enabled,
              delay,
              interval);

        this.endpoint = endpoint;
        this.method = method;
        this.retries = retries;
    }

    /**
     * An endpoint address for schema reporting; empty by default
     */
    @Override
    @Nullable
    public String endpoint()
    {
        return endpoint;
    }

    /**
     * An HTTP verb to use when reporting schemata; {@code PUT} by default
     */
    @Override
    @Nullable
    public HttpMethod method()
    {
        return method != null ? HttpMethod.valueOf(method) : null;
    }

    /**
     * A number of times a schema report is retried in case of failure; {@code 3} by default
     */
    @Override
    public int retries()
    {
        return retries;
    }
}
