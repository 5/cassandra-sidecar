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

package org.apache.cassandra.sidecar.restore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.SidecarRateLimiter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.data.StorageCredentials;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.assertj.core.data.Percentage;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.BinaryUtils;
import software.amazon.awssdk.utils.Md5Utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

class StorageClientTest
{
    private static final int LARGE_FILE_IN_BYTES = 1024 * 1024;
    private static final String testBucket = "bucket";
    private static final String testData = "testData";
    private static final String checksum = BinaryUtils.toHex(Md5Utils.computeMD5Hash(testData.getBytes()));
    private static final String largeTestFileName = "largeTestFile";
    private static final String testEncKeyRef =
    "arn:aws:kms:us-east-1:1234567890:key/valid-test-key-ref";
    private static S3MockContainer s3Mock;
    private static S3AsyncClient s3AsyncClient;
    private static StorageClient client;
    private static RestoreJob restoreJob;
    private static RestoreRange testRange;
    private static RestoreRange largeTestRange;
    private static Path largeFilePath;
    private static Vertx vertx;
    private static ExecutorPools executorPools;
    private static TaskExecutorPool taskExecutorPool;

    @TempDir
    private static Path testFolder;

    @BeforeAll
    static void setup() throws Exception
    {
        s3Mock = new S3MockContainer("3.5.1")
                 .withValidKmsKeys(testEncKeyRef)
                 .withInitialBuckets(testBucket);
        s3Mock.start();
        // test credential defined in s3mock
        StorageCredentials credentials = StorageCredentials.builder()
                                                           .accessKeyId("foo")
                                                           .secretAccessKey("bar")
                                                           .sessionToken("session")
                                                           .region("us-west-1").build();
        restoreJob = RestoreJob.builder()
                               .jobId(UUIDs.timeBased())
                               .jobStatus(RestoreJobStatus.CREATED)
                               .jobSecrets(new RestoreJobSecrets(credentials, credentials))
                               .sstableImportOptions(SSTableImportOptions.defaults())
                               .build();
        s3AsyncClient = buildS3AsyncClient(Duration.ofSeconds(60));
        client = new StorageClient(s3AsyncClient);
        client.authenticate(restoreJob);
        Path stageDirPath = testFolder.resolve("stage");
        testRange = getMockRange(restoreJob.jobId, testBucket, "key", checksum, stageDirPath, testData.length());
        putObject(testRange, testData);

        largeFilePath = prepareTestFile(testFolder, largeTestFileName, LARGE_FILE_IN_BYTES); // 1MB
        largeTestRange = getMockRange(restoreJob.jobId, testBucket, "largeKey",
                                      computeChecksum(largeFilePath), stageDirPath,
                                      LARGE_FILE_IN_BYTES);
        putObject(largeTestRange, largeFilePath);

        vertx = Vertx.vertx();
        executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        taskExecutorPool = executorPools.internal();
    }

    @AfterAll
    static void cleanup()
    {
        TestResourceReaper.create()
                          .with(executorPools)
                          .with(vertx)
                          .with(() -> s3Mock.stop(),
                                () -> client.close())
                          .close();
    }

    static S3AsyncClient buildS3AsyncClient(Duration apiCallTimeout) throws Exception
    {
        String httpsEndpoint = s3Mock.getHttpsEndpoint();
        return S3AsyncClient.builder()
                            .region(Region.US_WEST_1)
                            .overrideConfiguration(b -> b.apiCallTimeout(apiCallTimeout)
                                                         .apiCallAttemptTimeout(apiCallTimeout))
                            // provide a dummy credential to prevent client from identifying credentials
                            .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("foo", "bar")))
                            .endpointOverride(new URI(httpsEndpoint))
                            // required to prevent client from "manipulating" the object path
                            .forcePathStyle(true)
                            .httpClient(NettyNioAsyncHttpClient.builder().buildWithDefaults(
                            AttributeMap.builder()
                                        .put(TRUST_ALL_CERTIFICATES, Boolean.TRUE)
                                        .build()))
                            .build();
    }

    @Test
    void testUnauthenticated()
    {
        // slice from a new job that has not been authenticated
        RestoreRange unauthed = getMockRange(UUIDs.timeBased(), "newBucket", "newKey", null, null);
        assertThatThrownBy(() -> client.objectExists(unauthed).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No credential available");
    }

    @Test
    void testCheckObjectExistence() throws Exception
    {
        HeadObjectResponse response = client.objectExists(testRange).get();
        assertThat(response.sdkHttpResponse().statusCode()).isEqualTo(200);
        assertThat(response.eTag()).isEqualTo('"' + checksum + '"');
    }

    @Test
    void testCheckObjectExistenceChecksumMismatch()
    {
        RestoreRange withWrongChecksum = getMockRange(restoreJob.jobId, testBucket, "key", "wrong checksum", null);
        assertThatThrownBy(() -> client.objectExists(withWrongChecksum).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(S3Exception.class)
        .hasMessageContaining("Status Code: 412");
    }

    @Test
    void testCheckObjectExistenceNotFound()
    {
        RestoreRange notFound = getMockRange(restoreJob.jobId, testBucket, "keyNotFound", checksum, null);
        assertThatThrownBy(() -> client.objectExists(notFound).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(NoSuchKeyException.class);
    }

    @Test
    void testGetObject() throws Exception
    {
        File downloaded = client.downloadObjectIfAbsent(testRange, taskExecutorPool)
                                .toCompletionStage().toCompletableFuture().get();
        assertThat(downloaded.exists()).isTrue();
        assertThat(new String(Files.readAllBytes(downloaded.toPath()))).isEqualTo(testData);
    }

    @Test
    void testGetObjectHasExistingFileOnDisk() throws Exception
    {
        Path existingPath = testFolder.resolve(UUID.randomUUID().toString());
        Files.createDirectories(existingPath);
        Files.createFile(existingPath.resolve("key"));
        RestoreRange sliceHasFileOnDisk = getMockRange(restoreJob.jobId, testBucket, "key", checksum, existingPath);
        File downloaded = client.downloadObjectIfAbsent(sliceHasFileOnDisk, taskExecutorPool)
                                .toCompletionStage().toCompletableFuture().get();
        assertThat(downloaded.getAbsolutePath()).isEqualTo(existingPath.resolve("key").toString());
    }

    @Test
    void testGetObjectThroughputRateLimited() throws Exception
    {
        // only allow 1/4 the speed of transfer; each request downloads 128 KiB
        StorageClient client = new StorageClient(s3AsyncClient, 128 * 1024, SidecarRateLimiter.create(LARGE_FILE_IN_BYTES >> 2));
        client.authenticate(restoreJob);
        // Download should take around 4 seconds (256 KB/s for a 1MB file)
        long startNanos = System.nanoTime();
        File downloaded = client.downloadObjectIfAbsent(largeTestRange, taskExecutorPool)
                                .toCompletionStage().toCompletableFuture().get();
        assertThat(downloaded.exists()).isTrue();
        long elapsedNanos = System.nanoTime() - startNanos;
        assertThat(TimeUnit.NANOSECONDS.toMillis(elapsedNanos)).isCloseTo(TimeUnit.SECONDS.toMillis(4),
                                                                          Percentage.withPercentage(95));
        byte[] downloadedBytes = Files.readAllBytes(downloaded.toPath());
        byte[] originalBytes = Files.readAllBytes(largeFilePath);
        assertThat(Arrays.equals(downloadedBytes, originalBytes)).isTrue();
    }

    @Test
    void testApiCallTimeout() throws Exception
    {
        try (S3AsyncClient s3Client = buildS3AsyncClient(Duration.ofMillis(1)))
        {
            StorageClient client = new StorageClient(s3Client);
            client.authenticate(restoreJob);
            assertThatThrownBy(() -> client.objectExists(testRange).get())
            .hasMessageContaining(" Client execution did not complete before the specified timeout configuration: 1 millis")
            .hasRootCauseInstanceOf(ApiCallTimeoutException.class);
        }
    }

    private RestoreRange getMockRange(UUID jobId, String bucket, String key, String checksum, Path localPath)
    {
        return getMockRange(jobId, bucket, key, checksum, localPath, 0);
    }

    private static RestoreRange getMockRange(UUID jobId, String bucket, String key, String checksum, Path localPath, long length)
    {
        RestoreRange mock = mock(RestoreRange.class, RETURNS_DEEP_STUBS);
        when(mock.jobId()).thenReturn(jobId);
        when(mock.sliceBucket()).thenReturn(bucket);
        when(mock.sliceKey()).thenReturn(key);
        when(mock.sliceChecksum()).thenReturn(checksum);
        when(mock.stageDirectory()).thenReturn(localPath);
        when(mock.sliceObjectLength()).thenReturn(length);
        if (localPath != null)
        {
            when(mock.stagedObjectPath()).thenReturn(localPath.resolve(key));
        }
        return mock;
    }

    private static void putObject(RestoreRange range, String stringData) throws Exception
    {
        PutObjectRequest request = PutObjectRequest.builder()
                                                   .bucket(range.sliceBucket())
                                                   .key(range.sliceKey())
                                                   .build();

        s3AsyncClient.putObject(request, AsyncRequestBody.fromString(stringData)).get();
    }

    private static void putObject(RestoreRange range, Path path) throws Exception
    {
        PutObjectRequest request = PutObjectRequest.builder()
                                                   .bucket(range.sliceBucket())
                                                   .key(range.sliceKey())
                                                   .build();

        s3AsyncClient.putObject(request, AsyncRequestBody.fromFile(path)).get();
    }

    private static Path prepareTestFile(Path directory, String fileName, long sizeInBytes) throws IOException
    {
        Path filePath = directory.resolve(fileName);
        Files.deleteIfExists(filePath);

        byte[] buffer = new byte[1024];
        try (OutputStream outputStream = Files.newOutputStream(filePath))
        {
            int written = 0;
            while (written < sizeInBytes)
            {
                ThreadLocalRandom.current().nextBytes(buffer);
                int toWrite = (int) Math.min(buffer.length, sizeInBytes - written);
                outputStream.write(buffer, 0, toWrite);
                written += toWrite;
            }
        }

        return filePath;
    }

    private static String computeChecksum(Path path) throws IOException
    {
        try (InputStream inputStream = Files.newInputStream(path))
        {
            return BinaryUtils.toHex(Md5Utils.computeMD5Hash(inputStream));
        }
    }
}
