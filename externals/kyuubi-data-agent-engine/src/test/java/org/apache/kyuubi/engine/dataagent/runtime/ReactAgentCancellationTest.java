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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.chat.completions.ChatCompletionMessageParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentCancelled;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.event.EventType;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.AgentMiddleware;
import org.apache.kyuubi.engine.dataagent.runtime.middleware.Decision;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for {@link ReactAgent#cancelSession(String)}'s cancellation contract. Uses a
 * blocking middleware to stall the ReAct loop at {@code beforeLlmCall} — no real LLM is ever
 * contacted — then asserts that a Stop click on the owning session releases the agent thread within
 * a couple of seconds and terminates the run with a cancelled event.
 */
public class ReactAgentCancellationTest {

  /**
   * Dummy client that must never be invoked: every test in this class stalls the agent before it
   * reaches the LLM call. The dummy URL guarantees a loud failure if the loop-level checkpoints
   * ever regress and let a request slip through.
   */
  private static final OpenAIClient DUMMY_CLIENT =
      OpenAIOkHttpClient.builder()
          .apiKey("dummy-for-unit-tests")
          .baseUrl("http://127.0.0.1:0")
          .build();

  /**
   * Simulates a middleware that blocks the ReAct loop indefinitely (e.g. a rate-limiter waiting for
   * a token, a compactor summarizing over the network). It parks on a latch until {@code
   * ctx.isCancelled()} becomes true, then returns {@code Decision.abort} — exactly the shape a
   * cancellation-aware middleware should have.
   */
  private static final class BlockingUntilCancelledMiddleware implements AgentMiddleware {
    final CountDownLatch entered = new CountDownLatch(1);

    @Override
    public Decision<List<ChatCompletionMessageParam>> beforeLlmCall(
        AgentRunContext ctx, List<ChatCompletionMessageParam> messages) {
      entered.countDown();
      // Poll the cancel flag on a short interval — cheap and keeps the test deterministic.
      while (!ctx.isCancelled()) {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return Decision.abort("Interrupted");
        }
      }
      return Decision.abort("Cancelled by user");
    }
  }

  @Test
  public void testCloseSessionUnblocksAgentStuckInBeforeLlmCall() throws Exception {
    BlockingUntilCancelledMiddleware blocker = new BlockingUntilCancelledMiddleware();
    ReactAgent agent =
        ReactAgent.builder()
            .client(DUMMY_CLIENT)
            .modelName("dummy-model")
            .addMiddleware(blocker)
            .maxIterations(3)
            .systemPrompt("system")
            .build();

    List<AgentEvent> events = Collections.synchronizedList(new ArrayList<>());
    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      Future<?> run =
          exec.submit(
              () ->
                  agent.run(
                      new AgentInvocation("hi").sessionId("session-a"),
                      new ConversationMemory(),
                      events::add));

      // Wait until the middleware is actually blocking the ReAct loop.
      assertTrue(
          blocker.entered.await(2, TimeUnit.SECONDS),
          "Middleware must have stalled the agent thread");

      // Simulate the user clicking Stop.
      agent.cancelSession("session-a");

      // The agent thread must be released promptly — Stop is unusable otherwise.
      run.get(2, TimeUnit.SECONDS);
    } finally {
      exec.shutdownNow();
    }

    // The run must terminate as a cancellation, not as a middleware-abort error, and the
    // terminal AgentFinish event must still be emitted so listeners can release resources.
    AgentCancelled cancelled = firstOfType(events, AgentCancelled.class);
    assertNotNull(cancelled, "Expected an AgentCancelled event to be emitted");
    assertNotNull(cancelled.reason(), "AgentCancelled must carry a non-null reason");
    assertTrue(
        cancelled.reason().toLowerCase().contains("cancel"),
        "AgentCancelled reason should mention 'cancel', got: " + cancelled.reason());
    assertTrue(
        events.stream().anyMatch(e -> e.eventType() == EventType.AGENT_FINISH),
        "AgentFinish must be emitted even on cancel");
  }

  @Test
  public void testCloseSessionOnDifferentSessionDoesNotAffectThisRun() throws Exception {
    // Guards against over-eager cancellation: closing session-b must NOT release the run
    // that owns session-a.
    BlockingUntilCancelledMiddleware blocker = new BlockingUntilCancelledMiddleware();
    ReactAgent agent =
        ReactAgent.builder()
            .client(DUMMY_CLIENT)
            .modelName("dummy-model")
            .addMiddleware(blocker)
            .maxIterations(3)
            .systemPrompt("system")
            .build();

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      Future<?> run =
          exec.submit(
              () ->
                  agent.run(
                      new AgentInvocation("hi").sessionId("session-a"),
                      new ConversationMemory(),
                      e -> {}));

      assertTrue(blocker.entered.await(2, TimeUnit.SECONDS));

      // Close a DIFFERENT session; session-a's run must stay stalled.
      agent.cancelSession("session-b");
      // A bogus cancel would show up as an immediate completion; give it a chance and confirm
      // the run is still parked.
      try {
        run.get(200, TimeUnit.MILLISECONDS);
        throw new AssertionError("cancelSession(other) must not release this run");
      } catch (TimeoutException expected) {
        // ok: the run is still stalled, as it should be
      }

      // Clean up: cancel the right session so the test doesn't leak the agent thread.
      agent.cancelSession("session-a");
      run.get(2, TimeUnit.SECONDS);
    } finally {
      exec.shutdownNow();
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T firstOfType(List<AgentEvent> events, Class<T> type) {
    for (AgentEvent e : events) {
      if (type.isInstance(e)) return (T) e;
    }
    return null;
  }
}
