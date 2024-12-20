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

import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * An implementation of {@link IdentifiersProvider} for use with {@link ManualConverter}
 */
final class ManualIdentifiers extends IdentifiersProvider
{
    private final static String ORGANIZATION = "ASE Cassandra";
    private final static String PLATFORM = "cassandra-apple";

    private final String environment;
    private final String application;
    private final String cluster;
    private final UUID identifier;

    public ManualIdentifiers(@NotNull final String environment,
                             @NotNull final String application,
                             @NotNull final String cluster,
                             @NotNull final UUID identifier)
    {
        this.environment = environment;
        this.application = application;
        this.cluster = cluster;
        this.identifier = identifier;
    }

    @Override
    @NotNull
    public String organization()
    {
        return ORGANIZATION;
    }

    @Override
    @NotNull
    public String platform()
    {
        return PLATFORM;
    }

    @Override
    @NotNull
    public String environment()
    {
        return environment;
    }

    @Override
    @NotNull
    public String application()
    {
        return application;
    }

    @Override
    @NotNull
    public String cluster()
    {
        return cluster;
    }

    @Override
    @NotNull
    public UUID identifier()
    {
        return identifier;
    }
}
