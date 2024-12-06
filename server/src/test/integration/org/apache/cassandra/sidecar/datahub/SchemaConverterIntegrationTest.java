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

import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Session;
import com.linkedin.data.DataList;
import com.linkedin.data.codec.JacksonDataCodec;
import org.apache.cassandra.sidecar.common.server.utils.IOUtils;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaConverterIntegrationTest extends IntegrationTestBase
{
    private static final IdentifiersProvider IDENTIFIERS = new IdentifiersProvider() {};
    private static final SchemaConverter CONVERTER = new SchemaConverter(IDENTIFIERS);
    private static final JacksonDataCodec CODEC = new JacksonDataCodec();

    @CassandraIntegrationTest
    void testSchemaUtils() throws IOException {
        waitForSchemaReady(10L, TimeUnit.SECONDS);
        createTestKeyspace();
        createTestTable("CREATE TABLE " + TEST_KEYSPACE + ".testtable ("
                + "identifier int PRIMARY KEY, "
                + "name ascii, "
                + "value float)" + WITH_COMPACTION_DISABLED + ";");
                // TODO: Add more different types, specifically nested UDTs and nested
                //       collection types (think map<string, map<string, int>>)

        // First, ensure the returned schema matches the reference one (ignoring whitespace characters)
        final String actualJson;
        try (final Session session = maybeGetSession())
        {
            actualJson = CONVERTER.extractSchema(session.getCluster());
        }
        final String expectedJson = IOUtils.readFully("/datahub/integration_test.json");

        assertThat(actualJson).isEqualToNormalizingWhitespace(expectedJson);

        // Also, make sure the returned schema produces the same DataHub objects (when deserialized)
        final DataList   actualData = CODEC.readList(new StringReader(actualJson));
        final DataList expectedData = CODEC.readList(new StringReader(expectedJson));

        assertThat(actualData).isEqualTo(expectedData);
    }
}
