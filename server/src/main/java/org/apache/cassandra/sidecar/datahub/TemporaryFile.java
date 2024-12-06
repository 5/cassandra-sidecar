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

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Temporary file for formatted Cassandra schema
 * (necessary because DataHub API does not support in-memory extraction)
 */
final class TemporaryFile implements AutoCloseable
{
    private static final String PREFIX = "cassandra-schema-";
    private static final String EXTENSION = ".json";

    @NotNull
    private final Logger logger;

    @NotNull
    public final Path path;

    /**
     * Creates a temporary file for formatted Cassandra schema
     */
    public TemporaryFile(@NotNull final Logger logger) throws IOException
    {
        this.logger = logger;

        try
        {
            path = Files.createTempFile(PREFIX, EXTENSION);
        }
        catch (final Exception exception)
        {
            throw new IOException("Cannot create a temporary file for schema extraction", exception);
        }
    }

    /**
     * Reads formatted Cassandra schema from the temporary file
     */
    @NotNull
    public String content() throws IOException
    {
        try
        {
            return new String(Files.readAllBytes(path));
        }
        catch (final Exception exception)
        {
            throw new IOException("Cannot read extracted schema from the temporary file", exception);
        }
    }

    /**
     * Deletes the temporary file with formatted Cassandra schema
     */
    @Override
    public void close()
    {
        try
        {
            Files.delete(path);
        }
        catch (final Exception exception)
        {
            logger.warn("Cannot delete the temporary file with extracted schema", exception);
        }
    }
}
