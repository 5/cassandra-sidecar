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

package org.apache.cassandra.sidecar.common.request;

import java.util.UUID;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;

/**
 * Represents a request to retrieve the status of a async operational job
 */
public class OperationalJobRequest extends JsonRequest<OperationalJobResponse>
{

    /**
     * Constructs a request to retrieve status for a specified operational job
     */
    public OperationalJobRequest(UUID jobId)
    {
        super(ApiEndpointsV1.OPERATIONAL_JOB_ROUTE
              .replaceAll(ApiEndpointsV1.OPERATIONAL_JOB_ID_PATH_PARAM, jobId.toString()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HttpMethod method()
    {
        return HttpMethod.GET;
    }
}
