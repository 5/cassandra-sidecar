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

package org.apache.cassandra.sidecar.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.server.Server;

/**
 * Periodically checks whether the key store file has changed. Triggers an update to the server's SSLOptions
 * whenever a file change has detected.
 */
public class KeyStoreCheckPeriodicTask implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyStoreCheckPeriodicTask.class);

    private final Vertx vertx;
    private final Server server;
    private final SslConfiguration configuration;
    private long lastModifiedTime = 0; // records the last modified timestamp

    public KeyStoreCheckPeriodicTask(Vertx vertx, Server server, SslConfiguration configuration)
    {
        this.vertx = vertx;
        this.server = server;
        this.configuration = configuration;
        maybeRecordLastModifiedTime();
    }

    /**
     * Skip check if the key store is not configured or if the key store should not be reloaded
     *
     * @return whether to {@link ScheduleDecision#SKIP} or {@link ScheduleDecision#EXECUTE} this task
     */
    @Override
    public ScheduleDecision scheduleDecision()
    {
        return shouldSkip()
               ? ScheduleDecision.SKIP
               : ScheduleDecision.EXECUTE;
    }

    @Override
    public DurationSpec delay()
    {
        return configuration.keystore().checkInterval();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        LOGGER.info("Running periodic key store checker");
        String keyStorePath = configuration.keystore().path();
        vertx.fileSystem().props(keyStorePath)
             .onSuccess(props -> {
                 long previousLastModifiedTime = lastModifiedTime;
                 if (props.lastModifiedTime() != previousLastModifiedTime)
                 {
                     LOGGER.info("Certificate file change detected for path={}, previousLastModifiedTime={}, " +
                                 "lastModifiedTime={}", keyStorePath, previousLastModifiedTime,
                                 props.lastModifiedTime());

                     server.updateSSLOptions(props.lastModifiedTime())
                           .onSuccess(v -> {
                               lastModifiedTime = props.lastModifiedTime();
                               LOGGER.info("Completed reloading certificates from path={}", keyStorePath);
                               promise.complete(); // propagate successful completion
                           })
                           .onFailure(cause -> {
                               LOGGER.error("Failed to reload certificate from path={}", keyStorePath, cause);
                               promise.fail(cause);
                           });
                 }
                 else
                 {
                     promise.complete(); // make sure to fulfill the promise
                 }
             })
             .onFailure(error -> {
                 LOGGER.warn("Unable to retrieve props for path={}", keyStorePath, error);
                 promise.fail(error);
             });
    }

    protected void maybeRecordLastModifiedTime()
    {
        if (shouldSkip())
        {
            return;
        }
        String keyStorePath = configuration.keystore().path();
        vertx.fileSystem().props(keyStorePath)
             .onSuccess(props -> lastModifiedTime = props.lastModifiedTime())
             .onFailure(err -> {
                 LOGGER.error("Unable to get lastModifiedTime for path={}", keyStorePath);
                 lastModifiedTime = -1;
             });
    }

    /**
     * Skip check if the key store is not configured or if the key store should not be reloaded
     *
     * @return {@code true} if the key store is not configured or if the keystore should not be reloaded,
     * {@code false} otherwise
     */
    private boolean shouldSkip()
    {
        return !configuration.isKeystoreConfigured()
               || !configuration.keystore().reloadStore();
    }
}
