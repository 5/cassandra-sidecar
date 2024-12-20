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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.Versions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A test tool for converting a batch of Cassandra cluster schemata using {@link SchemaConverter}
 */
@SuppressWarnings("NewClassNamingConvention")
final class ManualConverter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConverter.class);

    private static final String VERSION = "4.0";
    private static final String LOCALHOST = "localhost";
    private static final String DC = "DC";

    private static final String HOME = System.getProperty("user.home", "/Users/ysemchyshyn");
    private static final String BASE = "Downloads/raw_schemas_wpc";
    private static final String METADATA = "wpc_cluster_metadata";
    private static final String SCHEMATA = "schemas";
    private static final String CONVERTED = "converted";
    private static final String DOT = ".";
    private static final String TXT = "txt";
    private static final String JSON = "json";

    private static final String ENVIRONMENT = "env";
    private static final String APPLICATION = "app";
    private static final String IDENTIFIER = "uuid";

    private static final String DCS = "(?is)(?<=\\bNetworkTopologyStrategy',).*?(?=})";
    private static final String SEPARATOR = "(?is)\\n\\s*\\n";
    private static final String VIRTUAL = "/*";
    private static final Pattern SYSTEM = Pattern.compile("^CREATE\\s+(?:KEYSPACE|TABLE|CUSTOM\\s+INDEX\\s+\\S+\\s+ON)\\s+"
                                                        + "(?:system|sidecar_internal|cie_internal)[\\s._].*;$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    @NotNull
    private static UpgradeableCluster startCassandra() throws IOException {
        final Versions.Version version = Versions.find()
                                                 .getLatest(new Semver(VERSION, Semver.SemverType.LOOSE));

        return UpgradeableCluster.build()
                                 .withVersion(version)
                                 .withDC(DC, 1)
                                 .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL))
                                 .start();
    }

    @NotNull
    private static Cluster createDriver() {
        return Cluster.builder()
                      .addContactPoint(LOCALHOST)
                      .build();
    }

    @NotNull
    private static Session connectSession(@NotNull final Cluster driver) {
        return driver.connect();
    }

    @NotNull
    private static String readFile(@NotNull final Path file) throws IOException
    {
        final byte[] content = Files.readAllBytes(file);

        return new String(content);
    }

    private static void writeFile(@NotNull final Path file,
                                  @NotNull final String content) throws IOException
    {
        Files.write(file, content.getBytes());
    }

    @NotNull
    private static ManualIdentifiers parseIdentifiers(@NotNull final Map.Entry<String, Object> metadata)
    {
        final String cluster = metadata.getKey();
        final JsonObject identifiers = (JsonObject) metadata.getValue();
        final String environment = identifiers.getString(ENVIRONMENT);
        final String application = identifiers.getString(APPLICATION);
        final UUID identifier = UUID.fromString(identifiers.getString(IDENTIFIER));

        return new ManualIdentifiers(environment, application, cluster, identifier);
    }

    private static boolean isVirtual(@NotNull final String cql)
    {
        return cql.startsWith(VIRTUAL);
    }

    private static boolean isSystem(@NotNull final String cql)
    {
        return SYSTEM.matcher(cql)
                     .matches();
    }

    private static boolean neitherVirtualNorSystem(@NotNull final String cql)
    {
        return !isVirtual(cql)
            && !isSystem(cql);
    }

    @NotNull
    private static String renameDCs(@NotNull final String cql) {
        return cql.replaceAll(DCS, " '" + DC + "': '1' ");
    }

    @NotNull
    private static Stream<String> splitSchema(@NotNull final String schema) {
        return Arrays.stream(schema.split(SEPARATOR));
    }

    @NotNull
    private static String convertSchema(@NotNull final Cluster cluster,
                                        @NotNull final ManualIdentifiers identifiers) {
        return new SchemaConverter(identifiers).extractSchema(cluster);
    }

    @Test
    @SuppressWarnings("unused")
    void convertSchema() throws IOException
    {
        final Path metadata = Paths.get(HOME, BASE, METADATA + DOT + JSON);
        final String clusters = readFile(metadata);

        for (final Map.Entry<String, Object> cluster : new JsonObject(clusters))
        {
            final ManualIdentifiers identifiers = parseIdentifiers(cluster);

            try (final AbstractCluster<?> cassandra = startCassandra();
                 final Cluster driver = createDriver();
                 final Session session = connectSession(driver))
            {
                final Path input = Paths.get(HOME, BASE, SCHEMATA, identifiers.cluster() + DOT + TXT);
                String schema = readFile(input);

                schema = renameDCs(schema);

                splitSchema(schema)
                        .map(String::trim)
                        .filter(ManualConverter::neitherVirtualNorSystem)
                        .forEach(session::execute);

                schema = convertSchema(session.getCluster(), identifiers);

                final Path output = Paths.get(HOME, BASE, CONVERTED, identifiers.environment() + DOT
                                                                   + identifiers.application() + DOT
                                                                   + identifiers.cluster() + DOT + JSON);
                writeFile(output, schema);
            }
            catch (final Throwable throwable)
            {
                LOGGER.error("Error converting schema for cluster with identifiers={}", identifiers, throwable);
            }
        }
    }
}
