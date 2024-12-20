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

import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.Callback;
import datahub.client.Emitter;
import datahub.client.MetadataWriteResponse;
import datahub.event.EventFormatter;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.event.UpsertAspectRequest;
import datahub.shaded.jackson.annotation.JsonInclude;
import datahub.shaded.jackson.core.PrettyPrinter;
import datahub.shaded.jackson.core.util.DefaultIndenter;
import datahub.shaded.jackson.core.util.DefaultPrettyPrinter;
import datahub.shaded.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A custom implementation of DataHub {@link Emitter} that stores emitted
 * metadata into the provided {@link StringBuilder} using JSON format
 */
final class JsonEmitter implements Emitter, AutoCloseable
{
    private static final String OPEN = "[";
    private static final String INDENT = "\t";
    private static final String LINE = "\n";
    private static final String COMMA = ",";
    private static final String CLOSE = "]";

    private static final DefaultPrettyPrinter.Indenter INDENTER = new DefaultIndenter(INDENT, LINE);
    private static final PrettyPrinter PRINTER = new DefaultPrettyPrinter().withObjectIndenter(INDENTER)
                                                                           .withArrayIndenter(INDENTER);
    private static final ObjectMapper MAPPER = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    private static final JacksonDataTemplateCodec CODEC = new JacksonDataTemplateCodec(MAPPER.getFactory());
    private static final EventFormatter FORMATTER = new EventFormatter();

    private static final Future<MetadataWriteResponse> SUCCESS = CompletableFuture.completedFuture(null);

    static
    {
        CODEC.setPrettyPrinter(PRINTER);
    }

    private final StringBuilder json;

    public JsonEmitter(@NotNull final StringBuilder buffer)
    {
        // Empty the provided buffer without changing its capacity
        buffer.setLength(0);

        json = buffer.append(OPEN);
    }

    @Override
    public synchronized boolean testConnection()
    {
        throw new UnsupportedOperationException(getClass() + " does not support testConnection() operation");
    }

    @Override
    @NotNull
    public synchronized Future<MetadataWriteResponse> emit(@NotNull final List<UpsertAspectRequest> requests,
                                                           @Nullable final Callback callback)
    {
        throw new UnsupportedOperationException(getClass() + " does not support UpsertAspectRequest operations");
    }

    @Override
    @NotNull
    public synchronized Future<MetadataWriteResponse> emit(@NotNull final MetadataChangeProposalWrapper wrapper,
                                                           @Nullable final Callback callback) throws IOException
    {
        final MetadataChangeProposal proposal = FORMATTER.convert(wrapper);
        return emit(proposal, callback);
    }

    @Override
    @NotNull
    public synchronized Future<MetadataWriteResponse> emit(@NotNull final MetadataChangeProposal proposal,
                                                           @Nullable final Callback callback) throws IOException {
        if (callback != null)
        {
            throw new IllegalArgumentException(getClass() + " does not support emission with Callback");
        }

        final String aspect = CODEC.mapToString(proposal.data());
        json.append(LINE)
            .append(aspect)
            .append(COMMA);

        return SUCCESS;
    }

    @Override
    public synchronized void close()
    {
        final int length = json.length();
        if (length > OPEN.length())
        {
            // Remove the trailing comma from non-empty buffer
            json.setLength(length - COMMA.length());
        }

        json.append(LINE)
            .append(CLOSE);
    }
}
