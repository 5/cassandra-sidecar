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

package org.apache.cassandra.sidecar.common.server.utils;

import java.util.concurrent.Callable;
import java.util.function.Predicate;

import org.apache.cassandra.sidecar.common.server.ThrowingRunnable;
import org.jetbrains.annotations.NotNull;

/**
 * Collection of utility methods for understanding {@link Throwable} thrown better;
 * also enables more fluent handling of checked exceptions in lambda expressions
 */
public final class ThrowableUtils
{
    /**
     * Version of {@link Supplier} that can throw any checked exception
     *
     * @param <T> the type of results supplied by this supplier
     */
    @FunctionalInterface
    public interface Supplier<T>
    {
        T get() throws Exception;
    }

    /**
     * Version of {@link Consumer} that can throw any checked exception
     *
     * @param <T> the type of the input to the operation
     */
    @FunctionalInterface
    public interface Consumer<T>
    {
        void accept(final T object) throws Exception;
    }

    /**
     * Version of {@link Function} that can throw any checked exception
     *
     * @param <T> the type of the input to the function
     * @param <R> the type of the result of the function
     */
    @FunctionalInterface
    public interface Function<T, R>
    {
        R apply(final T object) throws Exception;
    }

    /**
     * Private constructor that prevents unnecessary instantiation
     *
     * @throws UnsupportedOperationException when called
     */
    private ThrowableUtils()
    {
        throw new UnsupportedOperationException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Helper method that wraps any checked exception thrown in {@link Supplier} with a new unchecked exception
     */
    @NotNull
    public static <T> java.util.function.Supplier<T> supplier(@NotNull final Supplier<T> supplier)
    {
        return () ->
        {
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

    /**
     * Helper method that wraps any checked exception thrown in {@link Consumer} with a new unchecked exception
     */
    @NotNull
    public static <T> java.util.function.Consumer<T> consumer(@NotNull final Consumer<T> consumer)
    {
        return object ->
        {
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

    /**
     * Helper method that wraps any checked exception thrown in {@link Function} with a new unchecked exception
     */
    @NotNull
    public static <T, R> java.util.function.Function<T, R> function(@NotNull final Function<T, R> function)
    {
        return object ->
        {
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

    /**
     * Run the {@code actionMayThrow} and wrap any {@link Exception} thrown in {@link RuntimeException}
     *
     * @param <R> return value type of the action
     * @param actionMayThrow action that may throw exceptions
     * @return value of type R
     */
    public static <R> R propagate(Callable<R> actionMayThrow)
    {
        try
        {
            return actionMayThrow.call();
        }
        catch (Exception cause)
        {
            throw new RuntimeException(cause);
        }
    }

    /**
     * Similar to {@link #propagate(Callable)}, but takes runnable-ish
     */
    public static void propagate(ThrowingRunnable actionMayThrow)
    {
        propagate(() -> {
            actionMayThrow.run();
            return null;
        });
    }

    /**
     * Get the first throwable in the exception chain that matches with the expected throwable class.
     * When there is circular exception reference, it tries to visit all exceptions in the chain at least once
     * to make sure whether the exception to find exists or not. If still not found, null is returned.
     *
     * @param <T> type of the exception to look up
     * @param throwable the top most exception to check
     * @param expectedCauseKlass expected cause class
     * @return the cause that matches with the cause class or null
     */
    public static <T extends Throwable> T getCause(Throwable throwable, Class<T> expectedCauseKlass)
    {
        return expectedCauseKlass.cast(getCause(throwable, expectedCauseKlass::isInstance));
    }

    /**
     * Get the first throwable in the exception chain that satisfies the predicate.
     * When there is circular exception reference, it tries to visit all exceptions in the chain at least once
     * to make sure whether the exception to find exists or not. If still not found, null is returned.
     *
     * @param throwable the top most exception to check
     * @param predicate predicate
     * @return the cause that satisfies the predicate or null
     */
    public static Throwable getCause(Throwable throwable, Predicate<Throwable> predicate)
    {
        if (throwable == null)
        {
            return null;
        }
        Throwable cause = throwable;
        Throwable fastTracer = getCause(cause, 1);
        Throwable stop = null;
        // Keep on looking up the cause until hitting the end of the exception chain or finding the interested cause
        // It also detects whether there is a circular reference by applying fast and slow steppers.
        while (cause != null && stop != cause)
        {
            if (predicate.test(cause))
            {
                return cause;
            }

            if (stop == null)
            {
                // once stop is set; updating fast tracer is no longer required
                if (cause == fastTracer)
                {
                    // Mark the position to stop, and continue tracing the cause up until hitting stop the next time.
                    // This way we are sure that all exceptions/causes are visited at least once.
                    stop = cause;
                }
                else
                {
                    fastTracer = getCause(fastTracer, 2);
                }
            }
            cause = getCause(cause, 1);
        }
        return null;
    }

    private static Throwable getCause(Throwable throwable, int depth)
    {
        Throwable t = throwable;
        while (depth-- > 0 && t != null)
        {
            t = t.getCause();
        }
        return t;
    }
}
