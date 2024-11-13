package org.apache.cassandra.sidecar.datahub;

import com.google.common.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

/**
 * An implementation of {@link IdentifiersProvider} used for testing
 */
final class TestIdentifiers extends IdentifiersProvider
{
    private static final String CLUSTER = "cluster";

    @Override
    @NotNull
    public String cluster()
    {
        return CLUSTER;
    }
}
