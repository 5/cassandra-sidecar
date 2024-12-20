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

import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

final class ManualConversionIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void convertSchema() throws IOException
    {
        final String organization = "ASE Cassandra";
        final String platform = "cassandra-apple";
        final String environment = "IF1";
        final String application = "fearshared30qa";
        final String cluster = "fearshared30qa";
        final UUID identifier = UUID.fromString("d5f90f94-5a67-48f4-b04f-cfd1f41d56c4");

        final Path input = Paths.get("/Users/ysemchyshyn/Downloads/raw_schemas_wpc/schemas/" + cluster + ".txt");
        final Path output = Paths.get("/Users/ysemchyshyn/Downloads/raw_schemas_wpc/converted/" + identifier + ".json");

        waitForSchemaReady(1L, TimeUnit.MINUTES);

        try (final Session session = maybeGetSession())
        {
            Arrays.stream(new String(Files.readAllBytes(input))
                  .replaceAll("(?is)\\bDC(?=\\d+\\b)", "datacenter")
                  .split("(?is)\\n\\s*\\n"))
                  .map(String::trim)
                  .filter(cql -> !cql.startsWith("/*"))
                  .filter(cql -> !cql.matches("(?is)^CREATE\\s+(?:KEYSPACE|TABLE|CUSTOM\\s+INDEX\\s+\\S+\\s+ON)\\s+(?:system|sidecar_internal|cie_internal)[_\\s\\.].*;$"))
                  .forEach(session::execute);

            Files.write(output, new SchemaConverter(new IdentifiersProvider() { @Override @NotNull public String organization() { return organization; }
                                                                                @Override @NotNull public String platform() { return platform; }
                                                                                @Override @NotNull public String environment() { return environment; }
                                                                                @Override @NotNull public String application() { return application; }
                                                                                @Override @NotNull public UUID cluster() { return identifier; } })
                 .extractSchema(session.getCluster())
                 .replaceAll("(?is)\\b(?:cluster\\d+|Test Cluster)\\b", cluster)
                 .getBytes());
        }
    }
}
