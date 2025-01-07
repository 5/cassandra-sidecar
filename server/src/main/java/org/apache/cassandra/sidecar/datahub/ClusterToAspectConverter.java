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

import com.datastax.driver.core.Metadata;
import com.linkedin.data.template.RecordTemplate;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

public abstract class ClusterToAspectConverter<T extends RecordTemplate> extends MetadataToAspectConverter<T>
{
    protected ClusterToAspectConverter(@NotNull final IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    @NotNull
    protected MetadataChangeProposalWrapper<T> wrap(@NotNull final String urn,
                                                    @NotNull final T aspect)
    {
        final String type = IdentifiersProvider.DATA_PLATFORM;

        return wrap(type, urn, aspect);
    }

    @NotNull
    public abstract MetadataChangeProposalWrapper<T> convert(@NotNull final Metadata cluster) throws Exception;
}
