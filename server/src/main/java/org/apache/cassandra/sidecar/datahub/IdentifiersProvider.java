/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.datahub;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * A simple interface of an identifier provider class that can
 */
public abstract class IdentifiersProvider
{
    protected static final String URN = "urn";
    protected static final String LI = "li";
    static final String DATA_PLATFORM = "dataPlatform";
    static final String DATA_PLATFORM_INSTANCE = "dataPlatformInstance";
    static final String CONTAINER = "container";
    static final String DATASET = "dataset";

    @NotNull
    public String organization()
    {
        return "Cassandra";
    }

    @NotNull
    public String platform()
    {
        return "cassandra";
    }

    @NotNull
    public String environment()
    {
        return "Environment";
    }

    @NotNull
    public String application()
    {
        return "Application";
    }

    @NotNull
    public UUID cluster()
    {
        return UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
    }

    /**
     * Private helper method for retrieving the URN value
     */
    @NotNull
    public String urnDataPlatform()
    {
        return String.format("%s:%s:%s:%s",
                URN,
                LI,
                DATA_PLATFORM,
                platform());
    }

    /**
     * Private helper method for retrieving the Instance value
     */
    @NotNull
    public String urnDataPlatformInstance()
    {
        return String.format("%s:%s:%s:(%s:%s)",
                URN,
                LI,
                DATA_PLATFORM_INSTANCE,
                urnDataPlatform(),
                cluster());
    }

    /**
     * Private helper method for retrieving the Container value
     */
    @NotNull
    public String urnContainer(@NotNull final KeyspaceMetadata keyspace)
    {
        return String.format("%s:%s:%s:%s_%s",
                URN,
                LI,
                CONTAINER,
                cluster(),
                keyspace.getName());
    }

    /**
     * Private helper method for retrieving the URN value
     */
    @NotNull
    public String urnDataset(@NotNull final TableMetadata table)
    {
        return String.format("%s:%s:%s:(%s,%s.%s.%s,%s)",
                URN,
                LI,
                DATASET,
                urnDataPlatform(),
                cluster(),
                table.getKeyspace().getName(),
                table.getName(),
                environment());
    }
}
