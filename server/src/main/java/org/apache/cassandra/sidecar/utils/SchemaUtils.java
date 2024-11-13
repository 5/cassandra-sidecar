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

package org.apache.cassandra.sidecar.utils;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.SubTypes;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
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
import org.apache.commons.codec.digest.DigestUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Utility class for converting a Cassandra schema into a DataHub-compliant JSON-formatted String.
 *
 * Note that the extensive usage of {@link Stream<>} types here enables late creation and early destruction
 * of DataHub aspect objects (since all of them can potentially take up to a gigabyte).
 */
public final class SchemaUtils
{
    // TODO(ysemchyshyn): This is the initial implementation of DataHub support, and there are a few things missing at the moment
    //  * Figure out where to obtain the environment name {@link ENVIRONMENT}
    //  * Figure out where to obtain the application name {@link APPLICATION}
    //  * Figure out where to obtain the cluster name {@link CLUSTER}
    //  * Figure out how to generate a unique identifier for the cluster {@link IDENTIFIER}
    //  * Figure out where to obtain the actual Cassandra schema {@link SCHEMA}
    private static final String APPLICATION = "app_name";
    private static final String CLUSTER     = "cluster_name";
    private static final String ENVIRONMENT = "PROD";
    private static final UUID   IDENTIFIER  = UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6");
    private static final String SCHEMA      = "CREATE TABLE sample_keyspace.sample_table (seid text PRIMARY KEY, brokerstatus boolean, creationdate timestamp, "
                                            + "creditservicesstatus boolean, dsid text, lastmodifieddate timestamp, partnerservicestatus boolean, paymentservic"
                                            + "esstatus boolean, peerpaymentstatus boolean, signature text, tsmstatus boolean) WITH additional_write_policy = '"
                                            + "99p' AND allow_auto_snapshot = true AND bloom_filter_fp_chance = 0.1n AND caching = {'keys': 'ALL', 'rows_per_pa"
                                            + "rtition': 'NONE'} AND cdc = false AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compactio"
                                            + "n.LeveledCompactionStrategy', 'enabled': 'true', 'max_threshold': '32', 'min_threshold': '4'} AND compression = "
                                            + "{'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND memtable = 'default'"
                                            + " AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND disable_christmas_patch = false AND disable_repairs"
                                            + " = false AND extensions = {} AND gc_grace_seconds = 864000 AND incremental_backups = true AND max_index_interval"
                                            + " = 2048 AND memtable_flush_period_in_ms = 0 AND min_index_interval = 128 AND read_repair = 'BLOCKING' AND specul"
                                            + "ative_retry = '99p';";

    // An instance of logger to use
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUtils.class);

    // Constant URNs used in the creation of DataHub aspects
    private static final String CONTAINER_URN = "urn:li:container";
    private static final String DATASET_URN   = "urn:li:dataset";
    private static final String INSTANCE_URN  = "urn:li:dataPlatformInstance";
    private static final String PLATFORM_URN  = "urn:li:dataPlatform:cassandra-apple";

    // Constant values used in the creation of DataHub aspects
    private static final String DATASET_VALUE = "dataset";
    private static final String TABLE_VALUE   = "table";
    private static final long   VERSION_VALUE = 1L;

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
    };

    /**
     * Private constructor that prevents unnecessary instantiation
     *
     * @throws IllegalStateException when called
     */
    private SchemaUtils()
    {
        throw new IllegalStateException(getClass() + " is a static utility class and shall not be instantiated");
    }

    /**
     * Public helper method for extracting and formatting the Cassandra schema
     *
     * @param keyspace {@link KeyspaceMetadata} to extract Cassandra schema from
     *
     * @return DataHub schema as a JSON-formatted {@link String}
     */
    @NotNull
    public static String extractSchema(@NotNull final KeyspaceMetadata keyspace)
    {
        try (final TemporaryFile file = new TemporaryFile(LOGGER))
        {
            final Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> schema = prepareSchema(keyspace);

            writeSchema(schema, file.path);

            return file.content();
        }
        catch (final Exception exception)
        {
            throw new RuntimeException("Cannot extract schema for keyspace " + keyspace.getName(), exception);
        }
    }

    /**
     * Private helper method for formatting Cassandra schema using Acryl DataHub client
     */
    @NotNull
    private static Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> prepareSchema(@NotNull final KeyspaceMetadata keyspace)
    {
        return keyspace.getTables().stream()
                .flatMap(SchemaUtils::prepareSchema);
    }

    /**
     * Private helper method for formatting Cassandra schema using Acryl DataHub client
     */
    @NotNull
    @SuppressWarnings("unchecked")
    private static Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> prepareSchema(@NotNull final TableMetadata table)
    {
        final Stream<Function<TableMetadata, ? extends RecordTemplate>> aspects = Stream.of(
                Throwing.function(SchemaUtils::prepareDatasetProperties),
                Throwing.function(SchemaUtils::prepareSchemaMetadata),
                Throwing.function(SchemaUtils::prepareContainer),
                Throwing.function(SchemaUtils::prepareSubTypes),
                Throwing.function(SchemaUtils::prepareDataPlatformInstance),
                Throwing.function(SchemaUtils::prepareBrowsePathsV2));

        final String dataset = getDataset(table);

        return aspects.map(aspect -> MetadataChangeProposalWrapper.builder()
                .entityType(DATASET_VALUE)
                .entityUrn(dataset)
                .upsert()
                .aspect(aspect.apply(table))
                .build());
    }

    /**
     * Private helper method for preparing the Dataset Properties aspect
     */
    @NotNull
    private static DatasetProperties prepareDatasetProperties(@NotNull final TableMetadata table)
    {
        return new DatasetProperties()
                .setName(table.getName())
                .setQualifiedName(table.getKeyspace().getName() + "." + table.getName())
                .setDescription(table.getOptions().getComment());
        // TODO(ysemchyshyn): It is desirable to also obtain access timestamps, but the necessary permissions may be lacking
        //      .setCreated(convertTime(TIMESTAMP))
        //      .setLastModified(convertTime(TIMESTAMP));
    }

    /**
     * Private helper method for preparing the Schema Metadata aspect
     */
    @NotNull
    private static SchemaMetadata prepareSchemaMetadata(@NotNull final TableMetadata table)
    {
        final SchemaFieldArray fields = new SchemaFieldArray();
        table.getColumns().stream()
                .flatMap(SchemaUtils::convertColumn)
                .forEach(fields::add);

        final SchemaMetadata.PlatformSchema schema = new SchemaMetadata.PlatformSchema();
        schema.setOtherSchema(new OtherSchema().setRawSchema(SCHEMA));
        final String hash = DigestUtils.sha1Hex(SCHEMA);

        return new SchemaMetadata()
                .setSchemaName(table.getName())
                .setPlatform(new DataPlatformUrn(PLATFORM_URN))
                .setVersion(VERSION_VALUE)
                .setFields(fields)
                .setPlatformSchema(schema)
                .setHash(hash);
    }

    /**
     * Private helper method for preparing the Container aspect
     */
    @NotNull
    private static Container prepareContainer(@NotNull final TableMetadata table) throws URISyntaxException {
        final String container = getContainer(table);

        return new Container()
                .setContainer(new Urn(container));
    }

    /**
     * Private helper method for preparing the Sub Type aspect
     */
    @NotNull
    private static SubTypes prepareSubTypes(@NotNull final TableMetadata table)
    {
        return new SubTypes()
                .setTypeNames(new StringArray(TABLE_VALUE));
    }

    /**
     * Private helper method for preparing the Data Platform Instance aspect
     */
    @NotNull
    private static DataPlatformInstance prepareDataPlatformInstance(@NotNull final TableMetadata table) throws URISyntaxException
    {
        final String instance = getInstance(table);

        return new DataPlatformInstance()
                .setPlatform(new Urn(PLATFORM_URN))
                .setInstance(new Urn(instance));
    }

    /**
     * Private helper method for preparing the Browse Paths v.2 aspect
     */
    @NotNull
    private static BrowsePathsV2 prepareBrowsePathsV2(@NotNull final TableMetadata table) throws URISyntaxException
    {
        final String container = getContainer(table);
        final BrowsePathEntryArray path = new BrowsePathEntryArray(
                new BrowsePathEntry().setId(ENVIRONMENT),
                new BrowsePathEntry().setId(APPLICATION),
                new BrowsePathEntry().setId(CLUSTER),
                new BrowsePathEntry()
                        .setId(container)
                        .setUrn(new Urn(container)));

        return new BrowsePathsV2()
                .setPath(path);
    }

    /**
     * Private helper method for retrieving the Instance value
     */
    @NotNull
    private static String getInstance(@NotNull final TableMetadata table)
    {
        return String.format("%s:%s",
                INSTANCE_URN,
                IDENTIFIER);
    }

    /**
     * Private helper method for retrieving the Container value
     */
    @NotNull
    private static String getContainer(@NotNull final TableMetadata table)
    {
        return String.format("%s:%s_%s",
                CONTAINER_URN,
                IDENTIFIER,
                table.getKeyspace().getName());
    }

    /**
     * Private helper method for retrieving the URN value
     */
    @NotNull
    private static String getDataset(@NotNull final TableMetadata table)
    {
        return String.format("%s:(%s,%s.%s.%s,%s)",
                DATASET_URN,
                PLATFORM_URN,
                IDENTIFIER,
                table.getKeyspace().getName(),
                table.getName(),
                ENVIRONMENT);
    }

    /**
     * Private helper method for converting a Java timestamp into the DataHub timestamp
     */
    @SuppressWarnings("unused")
    @NotNull
    private static TimeStamp convertTime(@NotNull final Instant javaTime)
    {
        return new TimeStamp()
                .setTime(javaTime.toEpochMilli());
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
    private static Stream<SchemaField> convertType(@NotNull final String name, @NotNull final DataType type, final boolean partition, final boolean key)
    {
        if (type instanceof UserType)
        {
            final UserType udt = (UserType) type;

            return udt.getFieldNames().stream()
                    .flatMap(field -> convertType(name + "." + field, udt.getFieldType(field), partition, key));
        }
        else
        {
            final DataType.Name cassandraType = type.getName();
            final SchemaFieldDataType datahubType = convertType(cassandraType);

            return Stream.of(new SchemaField()  // No call to {@link SchemaField.setDescription()} since column-level comments are not supported by Cassandra
                    .setFieldPath(name)
                    .setNullable(!partition)  // Everything is potentially nullable in Cassandra except for the partition key
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

        return new SchemaFieldDataType().setType(datahubType);
    }

    /**
     * Private helper method for extracting the Cassandra schema from DataStax driver
     */
    private static void writeSchema(@NotNull final Stream<MetadataChangeProposalWrapper<? extends RecordTemplate>> schema, @NotNull final Path file)
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

    /**
     * Temporary file for formatted Cassandra schema
     * (necessary because DataHub API does not support in-memory extraction)
     */
    private static class TemporaryFile implements AutoCloseable
    {
        private static final String PREFIX = "cassandra-schema-";
        private static final String EXTENSION = ".json";

        private final Logger logger;

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
}
