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

import com.datastax.driver.core.KeyspaceMetadata;
import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.data.template.SetMode;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Converter class for preparing the Browse Paths v.2 aspect for a given Cassandra keyspace
 */
public class KeyspaceToBrowsePathsV2Converter extends KeyspaceToAspectConverter<BrowsePathsV2>
{
    public KeyspaceToBrowsePathsV2Converter(@NotNull IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    @Override
    @NotNull
    public MetadataChangeProposalWrapper<BrowsePathsV2> convert(@NotNull KeyspaceMetadata keyspace)
    {
        String urn = identifiers.urnContainer(keyspace);

        BrowsePathsV2 aspect = new BrowsePathsV2().setPath(new BrowsePathEntryArray(
                new BrowsePathEntry()
                        .setId(identifiers.environment())
                        .setUrn(null, SetMode.REMOVE_IF_NULL),
                new BrowsePathEntry()
                        .setId(identifiers.application())
                        .setUrn(null, SetMode.REMOVE_IF_NULL),
                new BrowsePathEntry()
                        .setId(identifiers.cluster())
                        .setUrn(null, SetMode.REMOVE_IF_NULL)));

        return wrap(urn, aspect);
    }
}
