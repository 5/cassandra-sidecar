/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.utils;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link Throwing}
 */
class ThrowingTest
{
    /**
     * A new type of checked exception to throw for testing
     */
    private static class CheckedException extends Exception
    {
    }

    @Test
    @SuppressWarnings("Convert2MethodRef")
    void testThrowingSupplier()
    {
        Supplier<Void> supplier = Throwing.supplier(() ->
        {
            throw new CheckedException();
        });

        assertThatThrownBy(() -> supplier.get())
            .isInstanceOf(Exception.class)
            .hasCauseInstanceOf(CheckedException.class);
    }

    @Test
    void testThrowingConsumer()
    {
        Consumer<Void> consumer = Throwing.consumer(object ->
        {
            throw new CheckedException();
        });

        assertThatThrownBy(() -> consumer.accept(null))
            .isInstanceOf(Exception.class)
            .hasCauseInstanceOf(CheckedException.class);
    }

    @Test
    void testThrowingFunction()
    {
        Function<Void, Void> supplier = Throwing.function(object ->
        {
            throw new CheckedException();
        });

        assertThatThrownBy(() -> supplier.apply(null))
            .isInstanceOf(Exception.class)
            .hasCauseInstanceOf(CheckedException.class);
    }
}
