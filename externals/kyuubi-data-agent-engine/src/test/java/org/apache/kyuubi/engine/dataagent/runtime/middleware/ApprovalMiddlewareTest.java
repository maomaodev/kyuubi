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

package org.apache.kyuubi.engine.dataagent.runtime.middleware;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kyuubi.engine.dataagent.runtime.AgentRunContext;
import org.apache.kyuubi.engine.dataagent.runtime.ApprovalMode;
import org.apache.kyuubi.engine.dataagent.runtime.ConversationMemory;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.apache.kyuubi.engine.dataagent.runtime.event.ApprovalRequest;
import org.apache.kyuubi.engine.dataagent.runtime.event.EventType;
import org.apache.kyuubi.engine.dataagent.tool.AgentTool;
import org.apache.kyuubi.engine.dataagent.tool.ToolContext;
import org.apache.kyuubi.engine.dataagent.tool.ToolRegistry;
import org.apache.kyuubi.engine.dataagent.tool.ToolRiskLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ApprovalMiddlewareTest {

  private ToolRegistry registry;
  private List<AgentEvent> emittedEvents;

  @BeforeEach
  public void setUp() {
    registry = new ToolRegistry(30);
    registry.register(safeTool("safe_tool"));
    registry.register(destructiveTool("dangerous_tool"));
    emittedEvents = Collections.synchronizedList(new ArrayList<>());
  }

  // --- Auto-approve mode: all tools pass ---

  @Test
  public void testAutoApproveModeSkipsAllApproval() {
    ApprovalMiddleware mw = newApprovalMiddleware();
    AgentRunContext ctx = makeContext(ApprovalMode.AUTO_APPROVE);

    assertEquals(
        Decision.Kind.PROCEED, mw.beforeToolCall(ctx, invocation("tc1", "dangerous_tool")).kind());
    assertEquals(
        Decision.Kind.PROCEED, mw.beforeToolCall(ctx, invocation("tc2", "safe_tool")).kind());
    assertTrue(emittedEvents.isEmpty(), "No approval events should be emitted");
  }

  // --- Normal mode: safe auto-approved, destructive needs approval ---

  @Test
  public void testNormalModeAutoApprovesSafeTool() {
    ApprovalMiddleware mw = newApprovalMiddleware();
    AgentRunContext ctx = makeContext(ApprovalMode.NORMAL);

    assertEquals(
        Decision.Kind.PROCEED, mw.beforeToolCall(ctx, invocation("tc1", "safe_tool")).kind());
    assertTrue(emittedEvents.isEmpty());
  }

  @Test
  public void testNormalModeRequiresApprovalForDestructiveTool() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(5);
    AgentRunContext ctx = makeContext(ApprovalMode.NORMAL);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch eventEmitted = new CountDownLatch(1);
      // Capture the emitted event to get the requestId
      ctx.setEventEmitter(
          event -> {
            emittedEvents.add(event);
            eventEmitted.countDown();
          });

      Future<Decision<ToolInvocation>> future =
          exec.submit(() -> mw.beforeToolCall(ctx, invocation("tc1", "dangerous_tool")));

      // Wait for the approval request event
      assertTrue(eventEmitted.await(2, TimeUnit.SECONDS), "Approval event should be emitted");
      assertEquals(1, emittedEvents.size());
      assertEquals(EventType.APPROVAL_REQUEST, emittedEvents.get(0).eventType());

      ApprovalRequest req = (ApprovalRequest) emittedEvents.get(0);
      assertEquals("dangerous_tool", req.toolName());
      assertEquals(ToolRiskLevel.DESTRUCTIVE, req.riskLevel());

      // Approve
      assertTrue(mw.resolve(req.requestId(), true));
      assertEquals(
          Decision.Kind.PROCEED,
          future.get(2, TimeUnit.SECONDS).kind(),
          "Approved tool should proceed");
    } finally {
      exec.shutdownNow();
    }
  }

  @Test
  public void testDeniedToolReturnsToolCallDenial() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(5);
    AgentRunContext ctx = makeContext(ApprovalMode.NORMAL);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch eventEmitted = new CountDownLatch(1);
      ctx.setEventEmitter(
          event -> {
            emittedEvents.add(event);
            eventEmitted.countDown();
          });

      Future<Decision<ToolInvocation>> future =
          exec.submit(() -> mw.beforeToolCall(ctx, invocation("tc1", "dangerous_tool")));

      assertTrue(eventEmitted.await(2, TimeUnit.SECONDS));
      ApprovalRequest req = (ApprovalRequest) emittedEvents.get(0);

      // Deny
      assertTrue(mw.resolve(req.requestId(), false));
      Decision<ToolInvocation> decision = future.get(2, TimeUnit.SECONDS);
      assertEquals(Decision.Kind.ABORT, decision.kind());
      assertTrue(decision.reason().contains("denied"));
    } finally {
      exec.shutdownNow();
    }
  }

  // --- Strict mode: all tools need approval ---

  @Test
  public void testStrictModeRequiresApprovalForSafeTool() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(5);
    AgentRunContext ctx = makeContext(ApprovalMode.STRICT);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch eventEmitted = new CountDownLatch(1);
      ctx.setEventEmitter(
          event -> {
            emittedEvents.add(event);
            eventEmitted.countDown();
          });

      Future<Decision<ToolInvocation>> future =
          exec.submit(() -> mw.beforeToolCall(ctx, invocation("tc1", "safe_tool")));

      assertTrue(eventEmitted.await(2, TimeUnit.SECONDS));
      ApprovalRequest req = (ApprovalRequest) emittedEvents.get(0);
      assertEquals("safe_tool", req.toolName());

      assertTrue(mw.resolve(req.requestId(), true));
      assertEquals(Decision.Kind.PROCEED, future.get(2, TimeUnit.SECONDS).kind());
    } finally {
      exec.shutdownNow();
    }
  }

  // --- Timeout ---

  @Test
  public void testApprovalTimeoutReturnsDenial() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(1); // 1 second timeout
    AgentRunContext ctx = makeContext(ApprovalMode.STRICT);
    ctx.setEventEmitter(emittedEvents::add);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      Future<Decision<ToolInvocation>> future =
          exec.submit(() -> mw.beforeToolCall(ctx, invocation("tc1", "safe_tool")));

      // Don't resolve — let it time out
      Decision<ToolInvocation> decision = future.get(5, TimeUnit.SECONDS);
      assertEquals(Decision.Kind.ABORT, decision.kind(), "Timeout should produce a denial");
      assertTrue(decision.reason().contains("timed out"));
    } finally {
      exec.shutdownNow();
    }
  }

  // --- Cancel all ---

  @Test
  public void testOnStopUnblocksPendingRequests() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(30);
    AgentRunContext ctx = makeContext(ApprovalMode.STRICT);
    ctx.setEventEmitter(emittedEvents::add);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch started = new CountDownLatch(1);
      Future<Decision<ToolInvocation>> future =
          exec.submit(
              () -> {
                started.countDown();
                return mw.beforeToolCall(ctx, invocation("tc1", "safe_tool"));
              });

      assertTrue(started.await(2, TimeUnit.SECONDS));
      Thread.sleep(100); // let the thread enter the blocking wait

      mw.onStop();

      Decision<ToolInvocation> decision = future.get(2, TimeUnit.SECONDS);
      assertEquals(Decision.Kind.ABORT, decision.kind(), "onStop should unblock with a denial");
    } finally {
      exec.shutdownNow();
    }
  }

  // --- Per-session cancel (P0-3: pending approvals must be released on session cancel) ---

  /**
   * Reproduces the leak described in KYUUBI_DATA_AGENT.md P0-3: when a session is cancelled while
   * it still owns a pending approval, {@link AgentRunContext#cancel()} MUST release the blocked
   * agent thread instead of letting the {@code CompletableFuture} dangle until the 5-minute timeout
   * expires.
   */
  @Test
  public void testCtxCancelReleasesPendingApprovalForThatSession() throws Exception {
    // Configure a long timeout so that the ONLY way the future can complete quickly is
    // by an explicit ctx.cancel() call — not by the timeout path.
    ApprovalMiddleware mw = newApprovalMiddleware(30);
    AgentRunContext ctx = makeContext(ApprovalMode.STRICT, "session-a");

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch eventEmitted = new CountDownLatch(1);
      ctx.setEventEmitter(
          event -> {
            emittedEvents.add(event);
            eventEmitted.countDown();
          });

      Future<Decision<ToolInvocation>> future =
          exec.submit(() -> mw.beforeToolCall(ctx, invocation("tc1", "dangerous_tool")));

      // Ensure the middleware has actually entered the blocking wait.
      assertTrue(eventEmitted.await(2, TimeUnit.SECONDS), "Approval event should be emitted");

      // Simulate the upstream session being force-closed while the approval is still pending.
      ctx.cancel();

      // A correctly wired ctx.cancel() must release the blocked thread promptly
      // (well under the 30s approval timeout).
      Decision<ToolInvocation> decision = future.get(2, TimeUnit.SECONDS);
      assertEquals(
          Decision.Kind.ABORT,
          decision.kind(),
          "ctx.cancel() must unblock the pending approval owned by that context");
    } finally {
      exec.shutdownNow();
    }
  }

  /**
   * Guards against an over-eager fix: cancelling session A must NOT release approvals that belong
   * to session B. Per-session isolation is now enforced by ctx-scoped cancellation listeners — this
   * test verifies the scoping is correct.
   */
  @Test
  public void testCtxCancelDoesNotAffectOtherSessions() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(30);

    // Two sessions, each with its own pending approval.
    AgentRunContext ctxA = makeContext(ApprovalMode.STRICT, "session-a");
    AgentRunContext ctxB = makeContext(ApprovalMode.STRICT, "session-b");

    CountDownLatch aEmitted = new CountDownLatch(1);
    CountDownLatch bEmitted = new CountDownLatch(1);
    List<ApprovalRequest> requestsB = Collections.synchronizedList(new ArrayList<>());
    ctxA.setEventEmitter(
        event -> {
          emittedEvents.add(event);
          aEmitted.countDown();
        });
    ctxB.setEventEmitter(
        event -> {
          emittedEvents.add(event);
          if (event instanceof ApprovalRequest) {
            requestsB.add((ApprovalRequest) event);
          }
          bEmitted.countDown();
        });

    ExecutorService exec = Executors.newFixedThreadPool(2);
    try {
      Future<Decision<ToolInvocation>> futureA =
          exec.submit(() -> mw.beforeToolCall(ctxA, invocation("tcA", "dangerous_tool")));
      Future<Decision<ToolInvocation>> futureB =
          exec.submit(() -> mw.beforeToolCall(ctxB, invocation("tcB", "dangerous_tool")));

      assertTrue(aEmitted.await(2, TimeUnit.SECONDS), "session-a approval should be emitted");
      assertTrue(bEmitted.await(2, TimeUnit.SECONDS), "session-b approval should be emitted");

      // Cancel only session A.
      ctxA.cancel();

      // A must be released.
      Decision<ToolInvocation> decisionA = futureA.get(2, TimeUnit.SECONDS);
      assertEquals(Decision.Kind.ABORT, decisionA.kind());

      // B must still be blocked (not released by A's cancel).
      assertFalse(
          futureB.isDone(), "session-b approval must NOT be released when session-a is cancelled");

      // Clean up: resolve B so the executor can shut down promptly.
      assertFalse(requestsB.isEmpty(), "session-b should have produced an ApprovalRequest event");
      assertTrue(mw.resolve(requestsB.get(0).requestId(), false));
      Decision<ToolInvocation> decisionB = futureB.get(2, TimeUnit.SECONDS);
      assertEquals(Decision.Kind.ABORT, decisionB.kind());
    } finally {
      exec.shutdownNow();
    }
  }

  /**
   * Safety hazard: after a session is cancelled, a late {@code resolve(reqId, true)} must NOT
   * silently release the previously pending approval. Otherwise a stale UI (or a retried approval
   * request) could still authorize a destructive tool call the user believed was aborted. Contract:
   * once {@link AgentRunContext#cancel()} has fired, {@link ApprovalMiddleware#resolve} for that
   * request must return {@code false}.
   */
  @Test
  public void testResolveAfterCancelMustBeNoop() throws Exception {
    ApprovalMiddleware mw = newApprovalMiddleware(30);
    AgentRunContext ctx = makeContext(ApprovalMode.STRICT, "session-a");

    ExecutorService exec = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch eventEmitted = new CountDownLatch(1);
      ctx.setEventEmitter(
          event -> {
            emittedEvents.add(event);
            eventEmitted.countDown();
          });

      Future<Decision<ToolInvocation>> future =
          exec.submit(() -> mw.beforeToolCall(ctx, invocation("tc1", "dangerous_tool")));
      assertTrue(eventEmitted.await(2, TimeUnit.SECONDS), "Approval event should be emitted");
      ApprovalRequest req = (ApprovalRequest) emittedEvents.get(0);

      // Simulate the user clicking Stop: ctx is cancelled while approval is pending.
      ctx.cancel();

      // The blocked agent thread must have been released with an ABORT.
      Decision<ToolInvocation> decision = future.get(2, TimeUnit.SECONDS);
      assertEquals(Decision.Kind.ABORT, decision.kind());

      // A late Approve (stale UI, retry, direct endpoint call) MUST be a no-op — the
      // destructive tool call must not be authorized after the session was cancelled.
      assertFalse(
          mw.resolve(req.requestId(), true),
          "resolve() after ctx.cancel() must return false; a late Approve must not release "
              + "a previously cancelled destructive tool call");
    } finally {
      exec.shutdownNow();
    }
  }

  // --- Helpers ---

  private ApprovalMiddleware newApprovalMiddleware() {
    ApprovalMiddleware mw = new ApprovalMiddleware();
    mw.onRegister(registry);
    return mw;
  }

  private ApprovalMiddleware newApprovalMiddleware(long timeoutSeconds) {
    ApprovalMiddleware mw = new ApprovalMiddleware(timeoutSeconds);
    mw.onRegister(registry);
    return mw;
  }

  private AgentRunContext makeContext(ApprovalMode mode) {
    AgentRunContext ctx = new AgentRunContext(new ConversationMemory(), mode);
    ctx.setEventEmitter(emittedEvents::add);
    return ctx;
  }

  private AgentRunContext makeContext(ApprovalMode mode, String sessionId) {
    AgentRunContext ctx = new AgentRunContext(new ConversationMemory(), mode, sessionId);
    ctx.setEventEmitter(emittedEvents::add);
    return ctx;
  }

  private static ToolInvocation invocation(String id, String name) {
    return new ToolInvocation(id, name, Collections.emptyMap());
  }

  private static AgentTool<DummyArgs> safeTool(String name) {
    return new DummyTool(name, ToolRiskLevel.SAFE);
  }

  private static AgentTool<DummyArgs> destructiveTool(String name) {
    return new DummyTool(name, ToolRiskLevel.DESTRUCTIVE);
  }

  public static class DummyArgs {
    public String value;
  }

  private static class DummyTool implements AgentTool<DummyArgs> {
    private final String name;
    private final ToolRiskLevel riskLevel;

    DummyTool(String name, ToolRiskLevel riskLevel) {
      this.name = name;
      this.riskLevel = riskLevel;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String description() {
      return "dummy tool";
    }

    @Override
    public ToolRiskLevel riskLevel() {
      return riskLevel;
    }

    @Override
    public Class<DummyArgs> argsType() {
      return DummyArgs.class;
    }

    @Override
    public String execute(ToolContext ctx, DummyArgs args) {
      return "ok";
    }
  }
}
