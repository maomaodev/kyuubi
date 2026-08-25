/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.dataagent.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Cancellation-primitive contract tests for {@link AgentRunContext}. */
public class AgentRunContextCancellationTest {

  private static AgentRunContext newCtx() {
    return new AgentRunContext(new ConversationMemory(), ApprovalMode.NORMAL);
  }

  @Test
  public void testIsCancelledStartsFalse() {
    assertFalse(newCtx().isCancelled());
  }

  @Test
  public void testCancelFlipsFlagAndIsIdempotent() {
    AgentRunContext ctx = newCtx();
    ctx.cancel();
    ctx.cancel(); // must not throw
    assertTrue(ctx.isCancelled());
  }

  @Test
  public void testCancelClosesRegisteredHandle() throws Exception {
    AgentRunContext ctx = newCtx();
    Counter c = new Counter();
    try (AutoCloseable ignored = ctx.registerCloseOnCancel(c)) {
      ctx.cancel();
    }
    assertEquals(1, c.count, "cancel() must close the registered handle exactly once");
    assertTrue(ctx.isCancelled());
  }

  @Test
  public void testCancelSwallowsCloseException() throws Exception {
    // A misbehaving handle must not prevent cancel() from marking the context as cancelled.
    AgentRunContext ctx = newCtx();
    try (AutoCloseable ignored =
        ctx.registerCloseOnCancel(
            () -> {
              throw new RuntimeException("boom");
            })) {
      ctx.cancel();
    }
    assertTrue(ctx.isCancelled());
  }

  @Test
  public void testHandleAutoDetachesOnClose() throws Exception {
    // Simulates the LlmStreamClient try-with-resources flow: after the try block exits, a later
    // cancel() must NOT re-close the already-completed stream.
    AgentRunContext ctx = newCtx();
    Counter c = new Counter();
    try (AutoCloseable ignored = ctx.registerCloseOnCancel(c)) {
      // normal completion, no cancel
    }
    ctx.cancel();
    assertEquals(0, c.count, "handle scope closed => cancel() must not touch the detached stream");
  }

  @Test
  public void testCancelFromAnotherThreadIsVisible() throws Exception {
    AgentRunContext ctx = newCtx();
    Counter c = new Counter();
    try (AutoCloseable ignored = ctx.registerCloseOnCancel(c)) {
      Thread t = new Thread(ctx::cancel);
      t.start();
      t.join(2000);
      assertFalse(t.isAlive(), "cancel() must complete promptly");
    }
    assertTrue(ctx.isCancelled());
    assertEquals(1, c.count);
  }

  @Test
  public void testCancelBeforeRegisterClosesResourceEagerly() throws Exception {
    // If cancel() fires before registerCloseOnCancel() registers a handle, the handle must be
    // closed
    // synchronously at registration time so callers don't have to race-check isCancelled().
    AgentRunContext ctx = newCtx();
    ctx.cancel();
    Counter c = new Counter();
    try (AutoCloseable ignored = ctx.registerCloseOnCancel(c)) {
      assertTrue(ctx.isCancelled(), "caller is still expected to observe isCancelled()");
    }
    // Registered against an already-cancelled context => registerCloseOnCancel closed it eagerly,
    // and the returned scope handle is a no-op detach that must not double-close.
    assertEquals(
        1, c.count, "registerCloseOnCancel after cancel() must close the resource exactly once");
  }

  @Test
  public void testMultipleHandlesClosedInLifoOrder() throws Exception {
    // New contract (optimization 1): the context supports multiple concurrent cancel listeners
    // (e.g. the in-flight LLM stream AND an ApprovalMiddleware future waiting on ctx.cancel).
    // On cancel() they must be closed in LIFO order — inner scopes unwind before outer ones.
    AgentRunContext ctx = newCtx();
    List<String> closedOrder = new ArrayList<>();
    try (AutoCloseable outer = ctx.registerCloseOnCancel(() -> closedOrder.add("outer"));
        AutoCloseable inner = ctx.registerCloseOnCancel(() -> closedOrder.add("inner"))) {
      ctx.cancel();
    }
    assertEquals(
        Arrays.asList("inner", "outer"),
        closedOrder,
        "cancel() must close registered handles in LIFO order");
    assertTrue(ctx.isCancelled());
  }

  private static final class Counter implements AutoCloseable {
    int count;

    @Override
    public void close() {
      count++;
    }
  }
}
