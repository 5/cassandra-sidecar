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

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.SubTypes;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.PlatformType;
import com.linkedin.dataplatforminstance.DataPlatformInstanceProperties;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.DateType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.NullType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.schema.TimeType;
import datahub.client.file.FileEmitter;
import datahub.client.file.FileEmitterConfig;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.cassandra.sidecar.utils.Throwing;
import org.apache.commons.codec.digest.DigestUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Utility class for converting a Cassandra schema into a DataHub-compliant JSON-formatted String.
 * <p>
 * Note that the extensive usage of {@link Stream<>} types here enables late creation and early destruction
 * of DataHub aspect objects (since all of them can potentially take up to a gigabyte).
 */
@SuppressWarnings("unused")
public class SchemaConverter
{
    // An instance of logger to use
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConverter.class);

    // Regular expression used to extract or validate {@code CREATE TABLE} statements
    private static final Pattern TABLE_SCHEMA = Pattern.compile("\\bCREATE\\s+TABLE\\s+(\\S+).*?;",
                                                                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    // Custom property names used in creation of some DataHub aspects
    public static final String ENVIRONMENT_PROPERTY = "environment";
    public static final String APPLICATION_PROPERTY = "application";
    public static final String CLUSTER_PROPERTY     = "cluster";

    // Constant values used in creation of some DataHub aspects
    private static final long   VERSION_VALUE   = 1L;
    private static final String DELIMITER_VALUE = ".";
    private static final String TABLE_VALUE     = "table";
    private static final String KEYSPACE_VALUE  = "keyspace";

    // Names of the data types recognized by the DataHub
    private static final SchemaFieldDataType.Type ARRAY_TYPE   = SchemaFieldDataType.Type.create(new ArrayType());
    private static final SchemaFieldDataType.Type BOOLEAN_TYPE = SchemaFieldDataType.Type.create(new BooleanType());
    private static final SchemaFieldDataType.Type BYTES_TYPE   = SchemaFieldDataType.Type.create(new BytesType());
    private static final SchemaFieldDataType.Type DATE_TYPE    = SchemaFieldDataType.Type.create(new DateType());
    private static final SchemaFieldDataType.Type MAP_TYPE     = SchemaFieldDataType.Type.create(new MapType());
    private static final SchemaFieldDataType.Type NULL_TYPE    = SchemaFieldDataType.Type.create(new NullType());
    private static final SchemaFieldDataType.Type NUMBER_TYPE  = SchemaFieldDataType.Type.create(new NumberType());
    private static final SchemaFieldDataType.Type STRING_TYPE  = SchemaFieldDataType.Type.create(new StringType());
    private static final SchemaFieldDataType.Type TIME_TYPE    = SchemaFieldDataType.Type.create(new TimeType());

    // Mappings from each Cassandra data type into the corresponding DataHub data type
    private static final Map<DataType.Name, SchemaFieldDataType.Type> TYPES = new HashMap<>();

    static
    {
        TYPES.put(DataType.Name.ASCII,     STRING_TYPE);
        TYPES.put(DataType.Name.BIGINT,    NUMBER_TYPE);
        TYPES.put(DataType.Name.BLOB,      BYTES_TYPE);
        TYPES.put(DataType.Name.BOOLEAN,   BOOLEAN_TYPE);
        TYPES.put(DataType.Name.COUNTER,   NUMBER_TYPE);
        TYPES.put(DataType.Name.DATE,      DATE_TYPE);
        TYPES.put(DataType.Name.DECIMAL,   NUMBER_TYPE);
        TYPES.put(DataType.Name.DOUBLE,    NUMBER_TYPE);
        TYPES.put(DataType.Name.FLOAT,     NUMBER_TYPE);
        TYPES.put(DataType.Name.INET,      STRING_TYPE);
        TYPES.put(DataType.Name.INT,       NUMBER_TYPE);
        TYPES.put(DataType.Name.LIST,      ARRAY_TYPE);
        TYPES.put(DataType.Name.MAP,       MAP_TYPE);
        TYPES.put(DataType.Name.SET,       ARRAY_TYPE);
        TYPES.put(DataType.Name.SMALLINT,  NUMBER_TYPE);
        TYPES.put(DataType.Name.TEXT,      STRING_TYPE);
        TYPES.put(DataType.Name.TIME,      TIME_TYPE);
        TYPES.put(DataType.Name.TIMESTAMP, DATE_TYPE);
        TYPES.put(DataType.Name.TIMEUUID,  STRING_TYPE);
        TYPES.put(DataType.Name.TINYINT,   NUMBER_TYPE);
        TYPES.put(DataType.Name.TUPLE,     ARRAY_TYPE);
        TYPES.put(DataType.Name.UUID,      STRING_TYPE);
        TYPES.put(DataType.Name.VARCHAR,   STRING_TYPE);
        TYPES.put(DataType.Name.VARINT,    NUMBER_TYPE);
    }

    // Instance fields that deterministically define the conversion behavior
    @NotNull
    protected final IdentifiersProvider identifiers;
    @NotNull
    protected final List<ClusterConverter> clusterConverters = new ArrayList<>();
    @NotNull
    protected final List<KeyspaceConverter> keyspaceConverters = new ArrayList<>();
    @NotNull
    protected final List<TableConverter> tableConverters = new ArrayList<>();
    protected String clusterName;

    @Inject
    public SchemaConverter(@NotNull final IdentifiersProvider identifiers)
    {
        this.identifiers = identifiers;
    }

    public void addClusterConverter(@NotNull final ClusterConverter converter)
    {
        clusterConverters.add(converter);
    }

    public void addKeyspaceConverter(@NotNull final KeyspaceConverter converter)
    {
        keyspaceConverters.add(converter);
    }

    public void addTableConverter(@NotNull final TableConverter converter)
    {
        tableConverters.add(converter);
    }

    /**
     * Public helper method for extracting and formatting the Cassandra schema
     *
     * @param cluster a {@link Cluster} to extract Cassandra schema from
     *
     * @return DataHub schema as a JSON-formatted {@link String}
     */
    @NotNull
    public String extractSchema(@NotNull final Cluster cluster)
    {
        this.clusterName = cluster.getMetadata().getClusterName();

        try (final TemporaryFile file = new TemporaryFile(LOGGER))
        {
            final Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> schema = prepareSchema(cluster.getMetadata());

            writeSchema(schema, file.path);

            return file.content();
        }
        catch (final Exception exception)
        {
            throw new RuntimeException("Cannot extract schema for cluster " + clusterName, exception);
        }
    }

    /**
     * Private helper method for preparing cluster schema using Acryl DataHub client
     */
    @NotNull
    @SuppressWarnings({"unchecked", "UnstableApiUsage"})
    private Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> prepareSchema(@NotNull final Metadata metadata)
    {
        final Stream<Function<Metadata, ? extends RecordTemplate>> aspects = Stream.of(
                Throwing.function(this::prepareDataPlatformInfo_C));

        Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> refactorMe = aspects.map(aspect -> MetadataChangeProposalWrapper.builder()
                .entityType(IdentifiersProvider.DATA_PLATFORM)
                .entityUrn(identifiers.urnDataPlatform())
                .upsert()
                .aspect(aspect.apply(metadata))
                .build());

        return Streams.concat(refactorMe,
                metadata.getKeyspaces().stream()
                        .filter(this::neitherVirtualNorSystem)
                        .flatMap(this::prepareSchema));
    }

    /**
     * Private helper method for preparing keyspace schema using Acryl DataHub client
     */
    @NotNull
    @SuppressWarnings({"unchecked", "UnstableApiUsage"})
    private Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> prepareSchema(@NotNull final KeyspaceMetadata keyspace)
    {
        final Stream<Function<KeyspaceMetadata, ? extends RecordTemplate>> aspects = Stream.of(
                Throwing.function(this::prepareContainerProperties_KS),
                Throwing.function(this::prepareDataPlatformInstance_KS),
                Throwing.function(this::prepareDataPlatformInstanceProperties_KS),
                Throwing.function(this::prepareBrowsePathsV2_KS));

        Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> refactorMe = aspects.map(aspect -> MetadataChangeProposalWrapper.builder()
                .entityType(IdentifiersProvider.CONTAINER)
                .entityUrn(identifiers.urnContainer(keyspace))
                .upsert()
                .aspect(aspect.apply(keyspace))
                .build());

        return Streams.concat(refactorMe,
                keyspace.getTables().stream()
                        .flatMap(this::prepareSchema));
    }

    /**
     * Private helper method for preparing table schema using Acryl DataHub client
     */
    @NotNull
    @SuppressWarnings("unchecked")
    private Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> prepareSchema(@NotNull final TableMetadata table)
    {
        final Stream<Function<TableMetadata, ? extends RecordTemplate>> aspects = Stream.of(
                Throwing.function(this::prepareDatasetProperties),
                Throwing.function(this::prepareSchemaMetadata),
                Throwing.function(this::prepareContainer),
                Throwing.function(this::prepareSubTypes),
                Throwing.function(this::prepareDataPlatformInstance),
                Throwing.function(this::prepareDataPlatformInstanceProperties),
                Throwing.function(this::prepareBrowsePathsV2));

        return aspects.map(aspect -> MetadataChangeProposalWrapper.builder()
                .entityType(IdentifiersProvider.DATASET)
                .entityUrn(identifiers.urnDataset(table))
                .upsert()
                .aspect(aspect.apply(table))
                .build());
    }

    /**
     * Private helper method for preparing the Data Platform Info aspect
     */
    @NotNull
    private DataPlatformInfo prepareDataPlatformInfo_C(@NotNull final Metadata cluster)
    {
        return new DataPlatformInfo()
                .setType(PlatformType.RELATIONAL_DB)
                .setName(identifiers.platform())
                .setDisplayName(identifiers.organization())
                .setDatasetNameDelimiter(DELIMITER_VALUE);
    }

    /**
     * Private helper method for preparing the Dataset Properties aspect
     */
    @NotNull
    private DatasetProperties prepareDatasetProperties(@NotNull final TableMetadata table)
    {
        DatasetProperties properties = new DatasetProperties()
                .setName(table.getName())
                .setQualifiedName(table.getKeyspace().getName() + DELIMITER_VALUE + table.getName());

        final String comment = table.getOptions().getComment();
        if (comment != null)
        {
            properties = properties
                .setDescription(comment);
        }

        // TODO: It is desirable to also obtain timestamps, but the necessary permissions may be lacking
        //       if (...)
        //       {
        //           properties = properties
        //               .setCreated(convertTime(...))
        //               .setLastModified(convertTime(...));
        //       }

        return properties;
    }

    /**
     * Private helper method for preparing the Schema Metadata aspect
     */
    @NotNull
    private SchemaMetadata prepareSchemaMetadata(@NotNull final TableMetadata table)
    {
        final SchemaFieldArray fields = new SchemaFieldArray();
        table.getColumns().stream()
                .flatMap(SchemaConverter::convertColumn)
                .forEach(fields::add);

        // Use {@code CREATE TABLE} CQL statement without associated UDTs or indexes as native schema
        final String cql = table.asCQLQuery();
        final SchemaMetadata.PlatformSchema schema = new SchemaMetadata.PlatformSchema();
        schema.setOtherSchema(new OtherSchema().setRawSchema(cql));
        final String hash = DigestUtils.sha1Hex(cql);

        return new SchemaMetadata()
                .setSchemaName(table.getName())
                .setPlatform(new DataPlatformUrn(identifiers.urnDataPlatform()))
                .setVersion(VERSION_VALUE)
                .setFields(fields)
                .setPlatformSchema(schema)
                .setHash(hash);
    }

    /**
     * Private helper method for preparing the Container aspect
     */
    @NotNull
    private Container prepareContainer(@NotNull final TableMetadata table) throws URISyntaxException {
        final String container = identifiers.urnContainer(table.getKeyspace());

        return new Container()
                .setContainer(new Urn(container));
    }

    /**
     * Private helper method for preparing the Sub Type aspect
     */
    @NotNull
    private SubTypes prepareSubTypes(@NotNull final TableMetadata table)
    {
        return new SubTypes()
                .setTypeNames(new StringArray(TABLE_VALUE));
    }

    /**
     * Private helper method for preparing the Data Platform Instance aspect
     */
    @NotNull
    private DataPlatformInstance prepareDataPlatformInstance(@NotNull final TableMetadata table) throws URISyntaxException
    {
        return new DataPlatformInstance()
                .setPlatform(new Urn(identifiers.urnDataPlatform()))
                .setInstance(new Urn(identifiers.urnDataPlatformInstance()));
    }

    /**
     * Private helper method for preparing the Data Platform Instance aspect
     */
    @NotNull
    private DataPlatformInstance prepareDataPlatformInstance_KS(@NotNull final KeyspaceMetadata keyspace) throws URISyntaxException
    {
        return new DataPlatformInstance()
                .setPlatform(new Urn(identifiers.urnDataPlatform()))
                .setInstance(new Urn(identifiers.urnDataPlatformInstance()));
    }

    /**
     * Private helper method for preparing the Container Properties aspect
     */
    @NotNull
    private ContainerProperties prepareContainerProperties_KS(@NotNull final KeyspaceMetadata keyspace)
    {
        return new ContainerProperties()
                .setName(keyspace.getName())
                .setDescription(null, SetMode.REMOVE_IF_NULL);  // Keyspace-level comments are not supported by Cassandra
    }

    /**
     * Private helper method for preparing the Data Platform Instance Properties aspect
     */
    @NotNull
    private DataPlatformInstanceProperties prepareDataPlatformInstanceProperties(@NotNull final TableMetadata table)
    {
        final Map<String, String> refactorMe = ImmutableMap.of(
                ENVIRONMENT_PROPERTY, identifiers.environment(),
                APPLICATION_PROPERTY, identifiers.application(),
                CLUSTER_PROPERTY,     clusterName);

        DataPlatformInstanceProperties properties = new DataPlatformInstanceProperties()
                .setName(table.getName())
                .setDescription(table.getOptions().getComment())
                .setCustomProperties(new StringMap(refactorMe));

        final String comment = table.getOptions().getComment();
        if (comment != null)
        {
            properties = properties
                .setDescription(comment);
        }

         return properties;
    }

    /**
     * Private helper method for preparing the Data Platform Instance Properties aspect
     */
    @NotNull
    private DataPlatformInstanceProperties prepareDataPlatformInstanceProperties_KS(@NotNull final KeyspaceMetadata keyspace)
    {
        final Map<String, String> properties = ImmutableMap.of(
                ENVIRONMENT_PROPERTY, identifiers.environment(),
                APPLICATION_PROPERTY, identifiers.application(),
                CLUSTER_PROPERTY,     clusterName);

        return new DataPlatformInstanceProperties()
                .setName(keyspace.getName())
                .setDescription(null, SetMode.REMOVE_IF_NULL)  // Keyspace-level comments are not supported by Cassandra
                .setCustomProperties(new StringMap(properties));
    }

    /**
     * Private helper method for preparing the Browse Paths v.2 aspect
     */
    @NotNull
    private BrowsePathsV2 prepareBrowsePathsV2(@NotNull final TableMetadata table) throws URISyntaxException
    {
        final String container = identifiers.urnContainer(table.getKeyspace());
        final BrowsePathEntryArray path = new BrowsePathEntryArray(
                new BrowsePathEntry()
                        .setId(identifiers.environment())
                        .setUrn(null, SetMode.REMOVE_IF_NULL),
                new BrowsePathEntry()
                        .setId(identifiers.application())
                        .setUrn(null, SetMode.REMOVE_IF_NULL),
                new BrowsePathEntry()
                        .setId(clusterName)
                        .setUrn(null, SetMode.REMOVE_IF_NULL),
                new BrowsePathEntry()
                        .setId(container)
                        .setUrn(new Urn(container)));

        return new BrowsePathsV2()
                .setPath(path);
    }

    /**
     * Private helper method for preparing the Browse Paths v.2 aspect
     */
    @NotNull
    private BrowsePathsV2 prepareBrowsePathsV2_KS(@NotNull final KeyspaceMetadata keyspace) throws URISyntaxException
    {
        final BrowsePathEntryArray path = new BrowsePathEntryArray(
                new BrowsePathEntry()
                        .setId(identifiers.environment())
                        .setUrn(null, SetMode.REMOVE_IF_NULL),
                new BrowsePathEntry()
                        .setId(identifiers.application())
                        .setUrn(null, SetMode.REMOVE_IF_NULL),
                new BrowsePathEntry()
                        .setId(clusterName)
                        .setUrn(null, SetMode.REMOVE_IF_NULL));

        return new BrowsePathsV2()
                .setPath(path);
    }

    /**
     * Private helper method for converting a Java timestamp into the DataHub timestamp
     */
    @NotNull
    private static TimeStamp convertTime(@NotNull final Instant javaTime)
    {
        return new TimeStamp()
                .setTime(javaTime.toEpochMilli());
    }

    /**
     * Protected helper method for filtering out virtual keyspaces, Cassandra system keyspaces, and Sidecar system keyspace
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
            && !name.equals("cie_internal")        // todo: push into apple-internal subclass or a configuration property
            && !name.startsWith("cie_internal_");  // todo: push into apple-internal subclass or a configuration property
    }

    /**
     * Private helper method for converting single Cassandra column metadata into a collection of at least one DataHub schema field definition
     */
    @NotNull
    private static Stream<SchemaField> convertColumn(@NotNull final ColumnMetadata column)
    {
        final DataType type = column.getType();
        final AbstractTableMetadata table = column.getParent();
        final boolean partition = table.getPartitionKey().contains(column);
        final boolean key = partition || table.getClusteringColumns().contains(column);  // Only check clustering key if needed

        return convertType(column.getName(), type, partition, key);
    }

    /**
     * Private helper method for converting Cassandra's single data type into a non-empty collection of DataHub's field definitions
     */
    @NotNull
    private static Stream<SchemaField> convertType(@NotNull final String name,
                                                   @NotNull final DataType type,
                                                   final boolean partition,
                                                   final boolean key)
    {
        if (type instanceof UserType)
        {
            final UserType udt = (UserType) type;

            return udt.getFieldNames().stream()
                    .flatMap(field -> convertType(name + DELIMITER_VALUE + field, udt.getFieldType(field), partition, key));
        }
        else
        {
            final DataType.Name cassandraType = type.getName();
            final SchemaFieldDataType datahubType = convertType(cassandraType);

            return Stream.of(new SchemaField()
                    .setFieldPath(name)
                    .setNullable(!partition)  // Everything is potentially nullable in Cassandra except for the partition key
                    .setDescription(null, SetMode.REMOVE_IF_NULL)  // Column-level comments are not supported by Cassandra
                    .setType(datahubType)
                    .setNativeDataType(cassandraType.toString().toLowerCase())
                    .setIsPartitioningKey(partition)
                    .setIsPartOfKey(key));
        }
    }

    /**
     * Private helper method for converting data types used by Cassandra into the ones recognized by DataHub,
     * uses {@code NullType} to indicate an unknown or unsupported data type
     */
    @NotNull
    private static SchemaFieldDataType convertType(@NotNull final DataType.Name cassandraType)
    {
        SchemaFieldDataType.Type datahubType = TYPES.get(cassandraType);
        if (datahubType == null)
        {
            datahubType = NULL_TYPE;  // Use the null type as an indicator of an unknown data type
            LOGGER.warn("Encountered an unknown data type " + cassandraType);
        }

        return new SchemaFieldDataType()
                .setType(datahubType);
    }

    /**
     * Private helper method for extracting the Cassandra schema from DataStax driver
     */
    private static void writeSchema(@NotNull final Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> schema,
                                    @NotNull final Path file)
    {
        final FileEmitterConfig config = FileEmitterConfig.builder()
                .fileName(file.toString())
                .build();

        try (final FileEmitter emitter = new FileEmitter(config))
        {
            schema.forEach(Throwing.consumer(emitter::emit));
        }
        catch (final Exception exception)
        {
            throw new RuntimeException("Cannot write extracted schema into the temporary file", exception);
        }
    }
}
