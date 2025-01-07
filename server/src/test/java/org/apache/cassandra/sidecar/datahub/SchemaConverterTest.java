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
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableOptionsMetadata;
import com.datastax.driver.core.UserType;
import com.google.common.collect.ImmutableList;
import com.sun.tools.javac.util.List;
import org.apache.cassandra.sidecar.common.server.utils.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SchemaConverter}
 */
final class SchemaConverterTest
{
    private static final IdentifiersProvider IDENTIFIERS = new IdentifiersProvider() {};
    private static final SchemaConverter CONVERTER = new SchemaConverter(IDENTIFIERS);

    @Test
    void testEmptyKeyspace() throws IOException
    {
        final Cluster cluster = mock(Cluster.class);
        final Metadata metadata = mock (Metadata.class);
        final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(cluster.getClusterName()).thenReturn("sample_cluster");
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspaces()).thenReturn(Collections.singletonList(keyspace));
        when(keyspace.getName()).thenReturn("sample_keyspace");
        when(keyspace.getTables()).thenReturn(Collections.emptyList());

        final String actual = CONVERTER.extractSchema(cluster);
        final String expected = IOUtils.readFully("/datahub/empty_keyspace.json");

        assertEquals(expected, actual);
    }

    @Test
    void testEmptyTable() throws IOException
    {
        final Cluster cluster = mock(Cluster.class);
        final Metadata metadata = mock (Metadata.class);
        final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        final TableMetadata table = mock(TableMetadata.class);
        final TableOptionsMetadata options = mock(TableOptionsMetadata.class);
        when(cluster.getClusterName()).thenReturn("sample_cluster");
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspaces()).thenReturn(Collections.singletonList(keyspace));
        when(keyspace.getName()).thenReturn("sample_keyspace");
        when(keyspace.getTables()).thenReturn(Collections.singletonList(table));
        when(table.getKeyspace()).thenReturn(keyspace);
        when(table.getName()).thenReturn("sample_table");
        when(table.getOptions()).thenReturn(options);
        when(table.exportAsString()).thenReturn("CREATE TABLE sample_keyspace.sample_table (...);");
        when(options.getComment()).thenReturn("table comment");

        final String actual = CONVERTER.extractSchema(cluster);
        final String expected = IOUtils.readFully("/datahub/empty_table.json");

        assertEquals(expected, actual);
    }

    @Test
    void testPrimitiveTypes() throws IOException
    {
        final Cluster cluster = mock(Cluster.class);
        final Metadata metadata = mock (Metadata.class);
        final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        final TableMetadata table = mock(TableMetadata.class);
        final TableOptionsMetadata options = mock(TableOptionsMetadata.class);
        final ColumnMetadata pk1 = mock(ColumnMetadata.class);
        final ColumnMetadata pk2 = mock(ColumnMetadata.class);
        final ColumnMetadata ck1 = mock(ColumnMetadata.class);
        final ColumnMetadata ck2 = mock(ColumnMetadata.class);
        final ColumnMetadata c1 = mock(ColumnMetadata.class);
        final ColumnMetadata c2 = mock(ColumnMetadata.class);
        final ColumnMetadata c3 = mock(ColumnMetadata.class);
        final ColumnMetadata c4 = mock(ColumnMetadata.class);
        final ColumnMetadata c5 = mock(ColumnMetadata.class);
        final ColumnMetadata c6 = mock(ColumnMetadata.class);
        final ColumnMetadata c7 = mock(ColumnMetadata.class);
        final ColumnMetadata c8 = mock(ColumnMetadata.class);
        when(cluster.getClusterName()).thenReturn("sample_cluster");
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspaces()).thenReturn(Collections.singletonList(keyspace));
        when(keyspace.getName()).thenReturn("sample_keyspace");
        when(keyspace.getTables()).thenReturn(ImmutableList.of(table));
        when(table.getKeyspace()).thenReturn(keyspace);
        when(table.getName()).thenReturn("sample_table");
        when(table.getOptions()).thenReturn(options);
        when(table.getColumns()).thenReturn(List.of(pk1, pk2, ck1, ck2, c1, c2, c3, c4, c5, c6, c7, c8));
        when(table.getPartitionKey()).thenReturn(List.of(pk1, pk2));
        when(table.getClusteringColumns()).thenReturn(List.of(ck1, ck2));
        when(table.exportAsString()).thenReturn("CREATE TABLE sample_keyspace.sample_table (...);");
        when(options.getComment()).thenReturn("table comment");
        when(pk1.getParent()).thenReturn(table);
        when(pk1.getName()).thenReturn("pk1");
        when(pk1.getType()).thenReturn(DataType.cint());
        when(pk2.getParent()).thenReturn(table);
        when(pk2.getName()).thenReturn("pk2");
        when(pk2.getType()).thenReturn(DataType.cfloat());
        when(ck1.getParent()).thenReturn(table);
        when(ck1.getName()).thenReturn("ck1");
        when(ck1.getType()).thenReturn(DataType.varint());
        when(ck2.getParent()).thenReturn(table);
        when(ck2.getName()).thenReturn("ck2");
        when(ck2.getType()).thenReturn(DataType.decimal());
        when(c1.getParent()).thenReturn(table);
        when(c1.getName()).thenReturn("c1");
        when(c1.getType()).thenReturn(DataType.cboolean());
        when(c2.getParent()).thenReturn(table);
        when(c2.getName()).thenReturn("c2");
        when(c2.getType()).thenReturn(DataType.date());
        when(c3.getParent()).thenReturn(table);
        when(c3.getName()).thenReturn("c3");
        when(c3.getType()).thenReturn(DataType.time());
        when(c4.getParent()).thenReturn(table);
        when(c4.getName()).thenReturn("c4");
        when(c4.getType()).thenReturn(DataType.ascii());
        when(c5.getParent()).thenReturn(table);
        when(c5.getName()).thenReturn("c6");
        when(c5.getType()).thenReturn(DataType.varchar());
        when(c6.getParent()).thenReturn(table);
        when(c6.getName()).thenReturn("c6");
        when(c6.getType()).thenReturn(DataType.blob());
        when(c7.getParent()).thenReturn(table);
        when(c7.getName()).thenReturn("c7");
        when(c7.getType()).thenReturn(DataType.list(DataType.uuid(), true));
        when(c8.getParent()).thenReturn(table);
        when(c8.getName()).thenReturn("c8");
        when(c8.getType()).thenReturn(DataType.map(DataType.timestamp(), DataType.inet(), false));

        final String actual = CONVERTER.extractSchema(cluster);
        final String expected = IOUtils.readFully("/datahub/primitive_types.json");

        assertEquals(expected, actual);
    }

    @Test
    void testUserTypes() throws IOException
    {
        final Cluster cluster = mock(Cluster.class);
        final Metadata metadata = mock (Metadata.class);
        final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        final TableMetadata table = mock(TableMetadata.class);
        final TableOptionsMetadata options = mock(TableOptionsMetadata.class);
        final ColumnMetadata pk = mock(ColumnMetadata.class);
        final ColumnMetadata ck = mock(ColumnMetadata.class);
        final ColumnMetadata udt1 = mock(ColumnMetadata.class);
        final ColumnMetadata udt2 = mock(ColumnMetadata.class);
        final ColumnMetadata c1 = mock(ColumnMetadata.class);
        final ColumnMetadata c2 = mock(ColumnMetadata.class);
        final UserType udt1t = mock(UserType.class);
        final UserType udt2t = mock(UserType.class);
        final UserType.Field udt1c1 = mock(UserType.Field.class);
        final UserType.Field udt1udt2 = mock(UserType.Field.class);
        final UserType.Field udt2c2 = mock(UserType.Field.class);
        when(cluster.getClusterName()).thenReturn("sample_cluster");
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspaces()).thenReturn(Collections.singletonList(keyspace));
        when(keyspace.getName()).thenReturn("sample_keyspace");
        when(keyspace.getTables()).thenReturn(ImmutableList.of(table));
        when(table.getKeyspace()).thenReturn(keyspace);
        when(table.getName()).thenReturn("sample_table");
        when(table.getOptions()).thenReturn(options);
        when(table.getColumns()).thenReturn(List.of(pk, ck, udt1));
        when(table.getPartitionKey()).thenReturn(List.of(pk));
        when(table.getClusteringColumns()).thenReturn(List.of(ck));
        when(table.exportAsString()).thenReturn("CREATE TABLE sample_keyspace.sample_table (...);");
        when(options.getComment()).thenReturn("table comment");
        when(pk.getParent()).thenReturn(table);
        when(pk.getName()).thenReturn("pk");
        when(pk.getType()).thenReturn(DataType.cint());
        when(ck.getParent()).thenReturn(table);
        when(ck.getName()).thenReturn("ck");
        when(ck.getType()).thenReturn(DataType.cfloat());
        when(udt1.getParent()).thenReturn(table);
        when(udt1.getName()).thenReturn("udt1");
        when(udt1.getType()).thenReturn(udt1t);
        when(udt2.getParent()).thenReturn(table);
        when(udt2.getName()).thenReturn("udt2");
        when(udt2.getType()).thenReturn(udt2t);
        when(c1.getParent()).thenReturn(table);
        when(c1.getName()).thenReturn("c1");
        when(c1.getType()).thenReturn(DataType.ascii());
        when(c2.getParent()).thenReturn(table);
        when(c2.getName()).thenReturn("c2");
        when(c2.getType()).thenReturn(DataType.cboolean());
        when(udt1t.getName()).thenReturn(DataType.Name.UDT);
        when(udt1t.getFieldNames()).thenReturn(List.of("c1", "udt2"));
        when(udt1t.getFieldType("c1")).thenReturn(DataType.ascii());
        when(udt1t.getFieldType("udt2")).thenReturn(udt2t);
        when(udt2t.getName()).thenReturn(DataType.Name.UDT);
        when(udt2t.getFieldNames()).thenReturn(List.of("c2"));
        when(udt2t.getFieldType("c2")).thenReturn(DataType.cboolean());
        when(udt1c1.getName()).thenReturn("c1");
        when(udt1c1.getType()).thenReturn(DataType.ascii());
        when(udt1udt2.getName()).thenReturn("udt2");
        when(udt1udt2.getType()).thenReturn(udt2t);
        when(udt2c2.getName()).thenReturn("c2");
        when(udt2c2.getType()).thenReturn(DataType.cboolean());

        final String actual = CONVERTER.extractSchema(cluster);
        final String expected = IOUtils.readFully("/datahub/user_types.json");

        assertEquals(expected, actual);
    }
}
