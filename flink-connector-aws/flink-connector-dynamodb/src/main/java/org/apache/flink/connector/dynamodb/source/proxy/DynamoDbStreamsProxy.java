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

package org.apache.flink.connector.dynamodb.source.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.ListShardsResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ExpiredIteratorException;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ShardFilter;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Implementation of the {@link StreamProxy} for DynamoDB streams. */
@Internal
public class DynamoDbStreamsProxy implements StreamProxy {

    private DynamoDbStreamsClient dynamoDbStreamsClient;
    private final SdkHttpClient httpClient;
    private final Map<String, String> shardIdToIteratorStore;
    private static final GetRecordsResponse EMPTY_GET_RECORDS_RESPONSE =
            GetRecordsResponse.builder()
                    .records(Collections.emptyList())
                    .nextShardIterator(null)
                    .build();

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStreamsProxy.class);

    // Circuit breaker parameters to prevent infinite client refreshes
    private static final int MAX_REFRESH_ATTEMPTS = 3;
    private static final long REFRESH_WINDOW_MS = 60000; // 1 minute
    private int refreshAttempts = 0;
    private long firstRefreshTimestamp = 0;

    public DynamoDbStreamsProxy(
            DynamoDbStreamsClient dynamoDbStreamsClient, SdkHttpClient httpClient) {
        this.dynamoDbStreamsClient = dynamoDbStreamsClient;
        this.httpClient = httpClient;
        this.shardIdToIteratorStore = new ConcurrentHashMap<>();
    }

    @Override
    public ListShardsResult listShards(String streamArn, @Nullable String lastSeenShardId) {
        try {
            ListShardsResult listShardsResult = new ListShardsResult();

            String lastEvaluatedShardId = lastSeenShardId;
            DescribeStreamResponse describeStreamResponse;
            do {
                describeStreamResponse = this.describeStream(streamArn, lastEvaluatedShardId);
                listShardsResult.addShards(describeStreamResponse.streamDescription().shards());
                listShardsResult.setStreamStatus(
                        describeStreamResponse.streamDescription().streamStatus());
                lastEvaluatedShardId =
                        describeStreamResponse.streamDescription().lastEvaluatedShardId();
                LOG.debug(
                        "DescribeStream lastEvaluatedShardId: {}, returned shards: {}",
                        lastEvaluatedShardId,
                        describeStreamResponse.streamDescription().shards());
            } while (describeStreamResponse.streamDescription().lastEvaluatedShardId() != null);

            return listShardsResult;
        } catch (DynamoDbException e) {
            if (isExpiredTokenException(e)) {
                LOG.info("Detected expired or invalid token. Refreshing client.");
                refreshClient();
                return listShards(streamArn, lastSeenShardId);
            }
            throw e;
        }
    }

    @Override
    public ListShardsResult listShardsWithFilter(String streamArn, ShardFilter shardFilter) {
        LOG.debug("Child shards with filter called, for shardId: {}", shardFilter.shardId());
        ListShardsResult listShardsResult = new ListShardsResult();

        try {
            DescribeStreamResponse describeStreamResponse =
                    this.describeStream(streamArn, shardFilter);
            listShardsResult.addShards(describeStreamResponse.streamDescription().shards());
            listShardsResult.setStreamStatus(
                    describeStreamResponse.streamDescription().streamStatus());
        } catch (DynamoDbException e) {
            if (isExpiredTokenException(e)) {
                LOG.info("Detected expired or invalid token. Refreshing client.");
                refreshClient();
                return listShardsWithFilter(streamArn, shardFilter);
            }
            LOG.warn("DescribeStream with Filter API threw an exception", e);
        } catch (Exception e) {
            LOG.warn("DescribeStream with Filter API threw an exception", e);
        }
        LOG.info("Child shards returned for shardId: {}", listShardsResult);
        return listShardsResult;
    }

    @Override
    public GetRecordsResponse getRecords(
            String streamArn, String shardId, StartingPosition startingPosition) {
        String shardIterator =
                shardIdToIteratorStore.computeIfAbsent(
                        shardId, (s) -> getShardIterator(streamArn, s, startingPosition));

        if (shardIterator == null) {
            return EMPTY_GET_RECORDS_RESPONSE;
        }
        try {
            GetRecordsResponse getRecordsResponse = getRecords(shardIterator);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        } catch (ExpiredIteratorException e) {
            LOG.info(
                    "Received ExpiredIteratorException from GetRecords. "
                            + "Calling GetShardIterator for shard: {} with position: {}",
                    startingPosition,
                    shardId);
            // Eagerly retry getRecords() if the iterator is expired
            shardIterator = getShardIterator(streamArn, shardId, startingPosition);
            GetRecordsResponse getRecordsResponse = getRecords(shardIterator);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        } catch (TrimmedDataAccessException e) {
            // TrimmedDataAccessException means that the record pointed by shard iterator has
            // expired.
            // We should read the shard back from TRIM_HORIZON
            LOG.warn(
                    "Received TrimmedDataAccessException from GetRecords. "
                            + "Calling GetShardIterator for shard: {} with TRIM_HORIZON",
                    shardId);
            shardIterator = getShardIterator(streamArn, shardId, StartingPosition.fromStart());
            GetRecordsResponse getRecordsResponse = getRecords(shardIterator);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        } catch (ResourceNotFoundException e) {
            LOG.warn(
                    "Received ResourceNotFoundException from GetRecords for shard: {}. "
                            + "This might indicate that there is restore happening from stale snapshot or data loss from backpressure",
                    shardId);
            return EMPTY_GET_RECORDS_RESPONSE;
        } catch (DynamoDbException e) {
            if (isExpiredTokenException(e)) {
                LOG.info("Detected expired or invalid token. Refreshing client.");
                refreshClient();
                return getRecords(streamArn, shardId, startingPosition);
            }
            throw e;
        }
    }

    private synchronized void refreshClient() {
        long currentTime = System.currentTimeMillis();

        // If this is the first refresh attempt or we're outside the window, reset the counter
        if (firstRefreshTimestamp == 0 || currentTime - firstRefreshTimestamp > REFRESH_WINDOW_MS) {
            refreshAttempts = 0;
            firstRefreshTimestamp = currentTime;
        }

        refreshAttempts++;

        if (refreshAttempts > MAX_REFRESH_ATTEMPTS) {
            String errorMsg =
                    String.format(
                            "Exceeded maximum number of client refresh attempts (%d) within time window (%d ms). "
                                    + "This may indicate a persistent credential issue.",
                            MAX_REFRESH_ATTEMPTS, REFRESH_WINDOW_MS);
            LOG.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }

        try {
            LOG.info(
                    "Closing existing DynamoDB Streams client due to expired credentials (attempt {} of {} within window)",
                    refreshAttempts,
                    MAX_REFRESH_ATTEMPTS);
            dynamoDbStreamsClient.close();
        } catch (Exception e) {
            LOG.warn("Error closing DynamoDB Streams client", e);
        }

        try {
            dynamoDbStreamsClient = DynamoDbStreamsClient.create();
            LOG.info("Created new DynamoDB Streams client with fresh credentials");
        } catch (Exception e) {
            LOG.error("Failed to create new DynamoDB Streams client. This may indicate a non-temporary credential issue.", e);
            throw new RuntimeException("Failed to refresh DynamoDB Streams client due to credential issues", e);
        }
    }

    private boolean isExpiredTokenException(AwsServiceException e) {
        String errorCode = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "";

        return "ExpiredToken".equalsIgnoreCase(errorCode)
                || "UnrecognizedClientException".equalsIgnoreCase(errorCode)
                || (e.getMessage() != null
                        && e.getMessage().contains("security token included in the request is expired"));
    }

    @Override
    public void close() throws IOException {
        dynamoDbStreamsClient.close();
        httpClient.close();
    }

    private DescribeStreamResponse describeStream(String streamArn, @Nullable String startShardId) {
        try {
            final DescribeStreamRequest describeStreamRequest =
                    DescribeStreamRequest.builder()
                            .streamArn(streamArn)
                            .exclusiveStartShardId(startShardId)
                            .build();

            DescribeStreamResponse describeStreamResponse =
                    dynamoDbStreamsClient.describeStream(describeStreamRequest);

            StreamStatus streamStatus = describeStreamResponse.streamDescription().streamStatus();
            if (streamStatus.equals(StreamStatus.ENABLING)) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn(
                            String.format(
                                    "The status of stream %s is %s ; result of the current "
                                            + "describeStream operation will not contain any shard information.",
                                    streamArn, streamStatus));
                }
            }

            return describeStreamResponse;
        } catch (DynamoDbException e) {
            if (isExpiredTokenException(e)) {
                LOG.info("Detected expired or invalid token. Refreshing client.");
                refreshClient();
                return describeStream(streamArn, startShardId);
            }
            throw e;
        }
    }

    private DescribeStreamResponse describeStream(String streamArn, ShardFilter shardFilter) {
        try {
            final DescribeStreamRequest describeStreamRequest =
                    DescribeStreamRequest.builder()
                            .streamArn(streamArn)
                            .shardFilter(shardFilter)
                            .build();

            DescribeStreamResponse describeStreamResponse =
                    dynamoDbStreamsClient.describeStream(describeStreamRequest);

            StreamStatus streamStatus = describeStreamResponse.streamDescription().streamStatus();
            if (streamStatus.equals(StreamStatus.ENABLING)) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn(
                            String.format(
                                    "The status of stream %s is %s ; result of the current "
                                            + "describeStream operation will not contain any shard information.",
                                    streamArn, streamStatus));
                }
            }

            return describeStreamResponse;
        } catch (DynamoDbException e) {
            if (isExpiredTokenException(e)) {
                LOG.info("Detected expired or invalid token. Refreshing client.");
                refreshClient();
                return describeStream(streamArn, shardFilter);
            }
            throw e;
        }
    }

    private String getShardIterator(
            String streamArn, String shardId, StartingPosition startingPosition) {
        GetShardIteratorRequest.Builder requestBuilder =
                GetShardIteratorRequest.builder()
                        .streamArn(streamArn)
                        .shardId(shardId)
                        .shardIteratorType(startingPosition.getShardIteratorType());

        switch (startingPosition.getShardIteratorType()) {
            case TRIM_HORIZON:
            case LATEST:
                break;
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                if (startingPosition.getStartingMarker() instanceof String) {
                    requestBuilder =
                            requestBuilder.sequenceNumber(
                                    (String) startingPosition.getStartingMarker());
                } else {
                    throw new IllegalArgumentException(
                            "Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER. Must be a String.");
                }
        }

        try {
            return dynamoDbStreamsClient.getShardIterator(requestBuilder.build()).shardIterator();
        } catch (ResourceNotFoundException e) {
            LOG.warn(
                    "Received ResourceNotFoundException from GetShardIterator. "
                            + "Shard {} of stream {} is no longer valid, marking it as complete."
                            + "This might indicate that there is restore happening from stale snapshot or data loss from backpressure",
                    shardId,
                    streamArn);
            return null;
        } catch (TrimmedDataAccessException e) {
            LOG.warn(
                    "Received TrimmedDataAccessException from GetShardIterator. "
                            + "Shard {} of stream {} is no longer valid, marking it as complete."
                            + "This might indicate that there is restore happening from stale snapshot or data loss from backpressure",
                    shardId,
                    streamArn);
            return null;
        } catch (DynamoDbException e) {
            if (isExpiredTokenException(e)) {
                LOG.info("Detected expired or invalid token. Refreshing client.");
                refreshClient();
                return getShardIterator(streamArn, shardId, startingPosition);
            }
            throw e;
        }
    }

    private GetRecordsResponse getRecords(String shardIterator) {
        if (Objects.isNull(shardIterator)) {
            return EMPTY_GET_RECORDS_RESPONSE;
        }
        try {
            return dynamoDbStreamsClient.getRecords(
                    GetRecordsRequest.builder().shardIterator(shardIterator).build());
        } catch (DynamoDbException e) {
            if (isExpiredTokenException(e)) {
                LOG.info("Detected expired or invalid token. Refreshing client.");
                refreshClient();
                return getRecords(shardIterator);
            }
            throw e;
        }
    }
}
