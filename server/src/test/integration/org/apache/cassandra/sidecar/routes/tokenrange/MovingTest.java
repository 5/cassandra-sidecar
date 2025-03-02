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

package org.apache.cassandra.sidecar.routes.tokenrange;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Range;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.testing.TestTokenSupplier;
import org.apache.cassandra.sidecar.testing.bytebuddy.BBHelperMovingNode;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

/**
 * Node movement scenarios integration tests for token range replica mapping endpoint with the in-jvm dtest framework.
 */
@ExtendWith(VertxExtension.class)
public class MovingTest extends MovingBaseTest
{

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void retrieveMappingWithKeyspaceMovingNode(VertxTestContext context,
                                               ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMovingNode.reset();
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer((cl, num) -> BBHelperMovingNode.install(cl, num, MOVING_NODE_IDX));
            builder.withTokenSupplier(tokenSupplier);
        });

        long moveTarget = getMoveTargetToken(cluster);
        runMovingTestScenario(context,
                              BBHelperMovingNode.transientStateStart,
                              BBHelperMovingNode.transientStateEnd,
                              cluster,
                              generateExpectedRangeMappingMovingNode(moveTarget),
                              moveTarget);
    }


    /**
     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
     * with the last node being moved by assigning it a different token
     * <p>
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     * <p>
     * In this test case, the moved node is inserted between nodes 1 and 2, resulting in splitting the ranges.
     */
    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingMovingNode(long moveTarget)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        List<Range<BigInteger>> expectedRanges = getMovingNodesExpectedRanges(annotation.nodesPerDc(),
                                                                              annotation.numDcs(),
                                                                              moveTarget);
        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
        // Initial range from Partitioner's MIN_TOKEN. This will include one of the replicas of the moved node since
        // it is adjacent to the range where it is being introduced.
        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3",
                                                         "127.0.0.5"));
        // Range including the token of the moved node. Node 5 is added here (and the preceding 3 ranges)
        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4",
                                                         "127.0.0.5"));
        // Split range resulting from the new token. This range is exclusive of the new token and node 5, and
        // has the same replicas as the previous range (as a result of the split)
        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.2", "127.0.0.3", "127.0.0.4"));
        // Node 1 is introduced here as it will take ownership of a portion of node 5's previous tokens as a result
        // of the move.
        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.3", "127.0.0.4", "127.0.0.5",
                                                         "127.0.0.1"));
        // Following 2 ranges remain unchanged as the replica-set remain the same post-move
        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.4", "127.0.0.5", "127.0.0.1"));
        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.2"));
        // Third (wrap-around) replica of the new location of node 5 is added to the existing replica-set
        mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3",
                                                         "127.0.0.5"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", mapping);
            }
        };
    }
}
