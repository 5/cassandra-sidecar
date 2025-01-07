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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.linkedin.data.template.RecordTemplate;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.cassandra.sidecar.utils.Throwing;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Stream;

/**
 * Utility class for converting a Cassandra schema into a DataHub-compliant JSON-formatted {@link String}.
 * <p>
 * Note that the extensive usage of {@link Stream<>} types here enables late creation and early destruction of DataHub aspect
 * objects while the schema is being converted (since all of them can potentially take up to a gigabyte on largest clusters).
 */
public class SchemaConverter
{
    protected static final int KB = 1024;
    protected static final int MB = KB * KB;

    @NotNull
    protected final IdentifiersProvider identifiers;
    @NotNull
    protected final List<ClusterToAspectConverter<? extends RecordTemplate>> clusterConverters;
    @NotNull
    protected final List<KeyspaceToAspectConverter<? extends RecordTemplate>> keyspaceConverters;
    @NotNull
    protected final List<TableToAspectConverter<? extends RecordTemplate>> tableConverters;

    /**
     * The public constructor that instantiates {@link SchemaConverter} with default configuration
     *
     * @param identifiers an instance of {@link IdentifiersProvider} to use
     */
    @Inject
    public SchemaConverter(@NotNull final IdentifiersProvider identifiers)
    {
        this(identifiers,
             ImmutableList.of(new ClusterToDataPlatformInfoConverter(identifiers),
                              new ClusterToDataPlatformInstancePropertiesConverter(identifiers)),
             ImmutableList.of(new KeyspaceToContainerPropertiesConverter(identifiers),
                              new KeyspaceToSubTypesConverter(identifiers),
                              new KeyspaceToDataPlatformInstanceConverter(identifiers),
                              new KeyspaceToBrowsePathsV2Converter(identifiers)),
             ImmutableList.of(new TableToDatasetPropertiesConverter(identifiers),
                              new TableToSchemaMetadataConverter(identifiers),
                              new TableToContainerConverter(identifiers),
                              new TableToSubTypesConverter(identifiers),
                              new TableToDataPlatformInstanceConverter(identifiers),
                              new TableToBrowsePathsV2Converter(identifiers)));
    }

    /**
     * A protected constructor that can be used to instantiate {@link SchemaConverter} with custom configuration
     *
     * @param identifiers an instance of {@link IdentifiersProvider} to use
     * @param clusterConverters a {@link List} of {@link ClusterToAspectConverter} instances to use
     * @param keyspaceConverters a {@link List} of {@link KeyspaceToAspectConverter} instances to use
     * @param tableConverters a {@link List} of {@link TableToAspectConverter} instances to use
     */
    protected SchemaConverter(@NotNull final IdentifiersProvider identifiers,
                              @NotNull final List<ClusterToAspectConverter<? extends RecordTemplate>> clusterConverters,
                              @NotNull final List<KeyspaceToAspectConverter<? extends RecordTemplate>> keyspaceConverters,
                              @NotNull final List<TableToAspectConverter<? extends RecordTemplate>> tableConverters)
    {
        this.identifiers = identifiers;
        this.clusterConverters = clusterConverters;
        this.keyspaceConverters = keyspaceConverters;
        this.tableConverters = tableConverters;
    }

    /**
     * Public method for extracting and formatting the Cassandra schema
     *
     * @param cluster a {@link Cluster} to extract Cassandra schema from
     *
     * @return DataHub schema as a JSON-formatted {@link String}
     */
    @NotNull
    public String extractSchema(@NotNull final Cluster cluster)
    {
        final StringBuilder json = new StringBuilder(MB);

        try (final JsonEmitter emitter = new JsonEmitter(json))
        {
            prepareSchema(cluster.getMetadata())
                    .forEach(Throwing.consumer(emitter::emit));
        }
        catch (final Exception exception)
        {
            throw new RuntimeException("Cannot extract schema for cluster " + identifiers.cluster(), exception);
        }

        return json.toString();
    }

    /**
     * Protected method for preparing cluster schema using Acryl DataHub client
     */
    @NotNull
    @SuppressWarnings("UnstableApiUsage")
    protected Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> prepareSchema(@NotNull final Metadata metadata)
    {
        return Streams.concat(
                clusterConverters.stream()
                        .map(Throwing.function(converter -> converter.convert(metadata))),
                metadata.getKeyspaces().stream()
                        .filter(this::neitherVirtualNorSystem)
                        .flatMap(this::prepareSchema));
    }

    /**
     * Protected method for preparing keyspace schema using Acryl DataHub client
     */
    @NotNull
    @SuppressWarnings("UnstableApiUsage")
    protected Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> prepareSchema(@NotNull final KeyspaceMetadata keyspace)
    {
        return Streams.concat(
                keyspaceConverters.stream()
                        .map(Throwing.function(converter -> converter.convert(keyspace))),
                keyspace.getTables().stream()
                        .flatMap(this::prepareSchema));
    }

    /**
     * Protected method for preparing table schema using Acryl DataHub client
     */
    @NotNull
    protected Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> prepareSchema(@NotNull final TableMetadata table)
    {
        return tableConverters.stream()
                .map(Throwing.function(converter -> converter.convert(table)));
    }

    /**
     * Protected method for filtering out virtual keyspaces, Cassandra system keyspaces, and Sidecar system keyspace
     */
    protected boolean neitherVirtualNorSystem(@NotNull final KeyspaceMetadata keyspace)
    {
        if (keyspace.isVirtual())
        {
            return false;
        }

        final String name = keyspace.getName();
        return !name.equals("system")
            && !name.startsWith("system_")
            && !name.equals("sidecar_internal")
            && !name.equals("cie_internal")        // TODO: Move into overriding method in the Apple-internal subclass
            && !name.startsWith("cie_internal_");  // TODO: Move into overriding method in the Apple-internal subclass
    }
}
