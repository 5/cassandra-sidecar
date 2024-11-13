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

import org.jetbrains.annotations.NotNull;

/**
 * Utility Class for Handling Checked Exceptions in Lambda Expressions
 */
public final class Throwing
{
    /**
     * Private constructor that prevents unnecessary instantiation
     *
     * @throws IllegalStateException when called
     */
    private Throwing()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    @FunctionalInterface
    public static abstract interface Supplier<T>
    {
        public abstract T get() throws Exception;
    }

    @NotNull
    public static <T> java.util.function.Supplier<T> supplier(@NotNull final Supplier<T> supplier)
    {
        return () -> {
            try
            {
                return supplier.get();
            }
            catch (final Exception exception)
            {
                throw new RuntimeException(exception);
            }
        };
    }

    @FunctionalInterface
    public static abstract interface Consumer<T>
    {
        public abstract void accept(final T object) throws Exception;
    }

    @NotNull
    public static <T> java.util.function.Consumer<T> consumer(@NotNull final Consumer<T> consumer)
    {
        return object -> {
            try
            {
                consumer.accept(object);
            }
            catch (final Exception exception)
            {
                throw new RuntimeException(exception);
            }
        };
    }

    @FunctionalInterface
    public static abstract interface Function<T, R>
    {
        public abstract R apply(final T object) throws Exception;
    }

    @NotNull
    public static <T, R> java.util.function.Function<T, R> function(@NotNull final Function<T, R> function)
    {
        return object -> {
            try
            {
                return function.apply(object);
            }
            catch (final Exception exception)
            {
                throw new RuntimeException(exception);
            }
        };
    }
}
