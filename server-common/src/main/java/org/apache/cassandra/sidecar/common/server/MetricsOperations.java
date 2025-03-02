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

package org.apache.cassandra.sidecar.common.server;

import org.apache.cassandra.sidecar.common.response.ConnectedClientStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.StreamsProgressStats;

/**
 * An interface that defines interactions with the metrics system in Cassandra.
 */
public interface MetricsOperations
{
    /**
     * Retrieve the connected client stats metrics from the cluster
     * @param summaryOnly boolean parameter to list connection summary only
     * @return the requested client stats, in full or summary
     */
    ConnectedClientStatsResponse connectedClientStats(boolean summaryOnly);

    /**
     * Retrieve the stream progress stats from the cluster
     * @return the requested stream progress stats
     */
    StreamsProgressStats streamsProgressStats();
}
