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

package org.apache.cassandra.sidecar.server;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.SharedMetricRegistries;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.TrafficShapingOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.apache.cassandra.sidecar.common.ResourceUtils.writeResourceToPath;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link Server} lifecycle
 */
@ExtendWith(VertxExtension.class)
class ServerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerTest.class);
    @TempDir
    private Path confPath;

    private Server server;
    private Vertx vertx;
    private WebClient client;
    private AtomicBoolean serverAlreadyClosedInTest = new AtomicBoolean(false);

    @BeforeEach
    void setup()
    {
        serverAlreadyClosedInTest.set(false);
        configureServer("config/sidecar_single_instance.yaml");
    }

    @AfterEach
    void tearDown()
    {
        SharedMetricRegistries.clear();
        if (server != null && !serverAlreadyClosedInTest.get())
        {
            try
            {
                server.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            catch (Exception ex)
            {
                LOGGER.error("Unable to close server after 5 seconds", ex);
            }
        }
        if (client != null)
        {
            client.close();
        }
    }

    @Test
    @DisplayName("Server should start and stop Sidecar successfully")
    void startStopServer(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint();
        Checkpoint serverStopped = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_STOP.address(), message -> serverStopped.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .compose(deploymentId -> server.stop(deploymentId))
              .onFailure(context::failNow);
    }

    @Test
    @DisplayName("Server should start and stop Sidecar successfully when there are no local instances configured")
    void startStopServerWithoutLocalInstances(VertxTestContext context)
    {
        // reconfigured the server with a yaml that has no local instances
        configureServer("config/sidecar_no_local_instances.yaml");

        Checkpoint serverStarted = context.checkpoint();
        Checkpoint serverStopped = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_STOP.address(), message -> serverStopped.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .compose(deploymentId -> server.stop(deploymentId))
              .onFailure(context::failNow);
    }

    @Test
    @DisplayName("Server should restart successfully")
    void testServerRestarts(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint(2);
        Checkpoint serverStopped = context.checkpoint(2);

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_STOP.address(), message -> serverStopped.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .compose(deploymentId -> server.stop(deploymentId))
              .compose(v -> server.start())
              .compose(this::validateHealthEndpoint)
              .compose(restartDeploymentId -> server.stop(restartDeploymentId))
              .onFailure(context::failNow);
    }

    @Test
    @DisplayName("Server should start and close Sidecar successfully")
    void startCloseServer(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint();
        Checkpoint serverStopped = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_STOP.address(), message -> serverStopped.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .compose(deploymentId -> {
                  serverAlreadyClosedInTest.set(true);
                  return server.close();
              })
              .onFailure(context::failNow);
    }

    @Test
    @DisplayName("Server should start and close Sidecar successfully and start is no longer allowed")
    void startCloseServerShouldNotStartAgain(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint();
        Checkpoint serverStopped = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_STOP.address(), message -> serverStopped.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .compose(deploymentId -> {
                  serverAlreadyClosedInTest.set(true);
                  return server.close();
              })
              .onSuccess(v -> assertThatException().isThrownBy(() -> server.start())
                                                   .withMessageContaining("Vert.x closed"))
              .onFailure(context::failNow);
    }

    @Test
    @DisplayName("Updating traffic shaping options should succeed")
    void updatingTrafficShapingOptions(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint();
        Checkpoint waitUntilUpdate = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .onFailure(context::failNow)
              .onSuccess(v -> {
                  TrafficShapingOptions update = new TrafficShapingOptions()
                                                 .setOutboundGlobalBandwidth(100 * 1024 * 1024);
                  server.updateTrafficShapingOptions(update);
                  waitUntilUpdate.flag();
                  context.completeNow();
              });
    }

    @Test
    @DisplayName("Updating traffic shaping options with non-zero listen port should succeed")
    void updateTrafficShapingOptionsWithNonZeroListenPort()
    {
        configureServer("config/sidecar_single_instance_non_zero_port.yaml");

        assertThatNoException().isThrownBy(() -> {
            server.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            TrafficShapingOptions update = new TrafficShapingOptions()
                                           .setOutboundGlobalBandwidth(100 * 1024 * 1024);
            server.updateTrafficShapingOptions(update);
        });
    }

    @Test
    @DisplayName("Server should stop immediately when there port is already in use")
    void stopImmediatelyWhenPortIsInUse(VertxTestContext context)
    {
        long attemptStartTimeMillis = System.currentTimeMillis();
        server.start()
              // simulate a bind exception due to address already in use
              .compose(deploymentId -> Future.failedFuture(new java.net.BindException("Address already in use")))
              .onComplete(context.failing(result -> {

                  try
                  {
                      serverAlreadyClosedInTest.set(true);
                      getBlocking(server.close(), 1, TimeUnit.MINUTES, "Stop server");
                  }
                  catch (Exception e)
                  {
                      context.failNow(e);
                      return;
                  }

                  long elapsedTimeMillis = System.currentTimeMillis() - attemptStartTimeMillis;
                  if (elapsedTimeMillis < TimeUnit.SECONDS.toMillis(10))
                  {
                      context.completeNow();
                  }
                  else
                  {
                      context.failNow("Expected server close to take less than 10000 millis, " +
                                      "but it took " + elapsedTimeMillis + " millis");
                  }
              }));
    }

    @Test
    @DisplayName("Update should fail with null options")
    void updatingTrafficShapingOptionsWithNull(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint();
        Checkpoint waitUntilUpdate = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .onFailure(context::failNow)
              .onSuccess(v -> {
                  assertThatIllegalArgumentException()
                  .isThrownBy(() -> server.updateTrafficShapingOptions(null))
                  .withMessage("Invalid null value passed for traffic shaping options update");
                  waitUntilUpdate.flag();
                  context.completeNow();
              });
    }

    @Test
    @DisplayName("Health endpoint specific metrics for server should be published")
    void healthEndpointMetricsPublished(VertxTestContext context)
    {
        Checkpoint serverStarted = context.checkpoint();
        Checkpoint waitUntilCheck = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());

        server.start()
              .compose(this::validateHealthEndpoint)
              .onFailure(context::failNow)
              .onSuccess(v -> {
                  vertx.setTimer(100, handle -> {
                      assertThat(registry().getMetrics().keySet().stream())
                      .anyMatch(name -> name.contains("/api/v1/__health"));
                      waitUntilCheck.flag();
                      context.completeNow();
                  });
              });
    }

    @Test
    @DisplayName("Invalid access control config, zero authenticators set")
    void invalidAccessControlConfig()
    {
        assertThatThrownBy(() -> configureServer("config/sidecar_invalid_accesscontrol_config.yaml"))
        .hasCauseInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Invalid access control configuration. There are no configured authenticators");
    }

    @Test
    @DisplayName("Invalid access control config, unrecognized authentication handler set")
    void unrecognizedAuthenticationHandlerSet()
    {
        assertThatThrownBy(() -> configureServer("config/sidecar_unrecognized_authenticator.yaml"))
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("Implementation for class org.apache.cassandra.sidecar.acl.authentication." +
                              "UnrecognizedAuthenticationHandler has not been registered");
    }

    @Test
    @DisplayName("Invalid access control config, unrecognized authorization provider set")
    void unrecognizedAuthorizationProviderSet()
    {
        assertThatThrownBy(() -> configureServer("config/sidecar_unrecognized_authorizer.yaml"))
        .hasCauseInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Unrecognized authorization provider org.apache.cassandra.sidecar.acl." +
                              "authorization.UnrecognizedAuthorizationProvider set");
    }

    Future<String> validateHealthEndpoint(String deploymentId)
    {
        LOGGER.info("Checking server health 127.0.0.1:{}/api/v1/__health", server.actualPort());
        return client.get(server.actualPort(), "127.0.0.1", "/api/v1/__health")
                     .send()
                     .compose(response -> {
                         assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                         assertThat(response.bodyAsJsonObject().getString("status")).isEqualTo("OK");
                         return Future.succeededFuture(deploymentId);
                     });
    }

    void configureServer(String sidecarYaml)
    {
        ClassLoader classLoader = ServerTest.class.getClassLoader();
        Path yamlPath = writeResourceToPath(classLoader, confPath, sidecarYaml);
        Injector injector = Guice.createInjector(new MainModule(yamlPath));
        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);
        client = WebClient.create(vertx);
    }
}
