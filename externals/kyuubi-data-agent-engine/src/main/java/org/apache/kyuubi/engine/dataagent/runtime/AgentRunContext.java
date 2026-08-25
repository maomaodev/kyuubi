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

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.kyuubi.engine.dataagent.runtime.event.AgentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mutable context passed through the middleware pipeline and agent loop. Tracks the current state
 * of agent execution including iteration count, token usage, and custom middleware state.
 */
public class AgentRunContext {

  private static final Logger LOG = LoggerFactory.getLogger(AgentRunContext.class);

  private final ConversationMemory memory;
  private final String sessionId;
  private Consumer<AgentEvent> eventEmitter;
  private int iteration;
  // accumulatedXxx: summed across every LLM call in this run (billing).
  // lastXxx: most recent LLM call only; UIs use lastPromptTokens as "current context size".
  // total is forwarded from the provider verbatim (not prompt+completion) because reasoning /
  // cached tokens don't show up in the split, and CompactionMiddleware needs the real total.
  private long accumulatedPromptTokens;
  private long accumulatedCompletionTokens;
  private long lastPromptTokens;
  private long lastCompletionTokens;
  private ApprovalMode approvalMode;

  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  /**
   * Resources to close on {@link #cancel()}. Multiple listeners may register concurrently (e.g. an
   * in-flight LLM stream and a middleware waiting on a future), {@link #cancel()} closes them in
   * LIFO order so inner scopes unwind before outer ones.
   */
  private final ConcurrentLinkedDeque<AutoCloseable> cancellationHandles =
      new ConcurrentLinkedDeque<>();

  public AgentRunContext(ConversationMemory memory, ApprovalMode approvalMode) {
    this(memory, approvalMode, null);
  }

  public AgentRunContext(ConversationMemory memory, ApprovalMode approvalMode, String sessionId) {
    this.memory = memory;
    this.iteration = 0;
    this.approvalMode = approvalMode;
    this.sessionId = sessionId;
  }

  public ConversationMemory getMemory() {
    return memory;
  }

  /**
   * The upstream session identifier this run belongs to. Threaded down from {@code
   * DataAgentProvider.run(sessionId, ...)}. May be {@code null} in unit tests that do not exercise
   * session-scoped middleware.
   */
  public String getSessionId() {
    return sessionId;
  }

  public int getIteration() {
    return iteration;
  }

  public void setIteration(int iteration) {
    this.iteration = iteration;
  }

  public long getAccumulatedPromptTokens() {
    return accumulatedPromptTokens;
  }

  public long getAccumulatedCompletionTokens() {
    return accumulatedCompletionTokens;
  }

  public long getLastPromptTokens() {
    return lastPromptTokens;
  }

  public long getLastCompletionTokens() {
    return lastCompletionTokens;
  }

  /**
   * Record one LLM call's usage. Updates both the per-run counters on this context and the
   * session-level cumulative on the underlying {@link ConversationMemory}, so middlewares that need
   * a session-wide picture can read it directly from memory without keeping their own bookkeeping.
   * The provider's {@code total} is forwarded as-is and may exceed {@code prompt + completion} when
   * the provider counts cached or reasoning tokens separately.
   */
  public void addTokenUsage(long prompt, long completion, long total) {
    this.accumulatedPromptTokens += prompt;
    this.accumulatedCompletionTokens += completion;
    this.lastPromptTokens = prompt;
    this.lastCompletionTokens = completion;
    memory.addCumulativeTokens(prompt, completion, total);
  }

  public ApprovalMode getApprovalMode() {
    return approvalMode;
  }

  public void setApprovalMode(ApprovalMode approvalMode) {
    this.approvalMode = approvalMode;
  }

  public void setEventEmitter(Consumer<AgentEvent> eventEmitter) {
    this.eventEmitter = eventEmitter;
  }

  /** Emit an event through the agent's event pipeline. Available for use by middlewares. */
  public void emit(AgentEvent event) {
    if (eventEmitter != null) {
      eventEmitter.accept(event);
    }
  }

  public boolean isCancelled() {
    return cancelled.get();
  }

  /**
   * Check the cancel flag and throw {@link AgentCancelledException} if set. Callers invoke this at
   * checkpoints instead of hand-rolling {@code if (ctx.isCancelled()) ...} so cancellation
   * propagates as a single typed exception that the agent loop's top-level handler catches once.
   */
  public void throwIfCancelled() {
    if (cancelled.get()) {
      throw new AgentCancelledException();
    }
  }

  /**
   * Register a closable resource to be closed when {@link #cancel()} fires. Returns a handle whose
   * {@link AutoCloseable#close()} detaches the registration (safe no-op after cancel).
   *
   * <p>The "push-then-recheck" pattern resolves the race with a concurrent {@code cancel()}:
   *
   * <ol>
   *   <li><b>No cancel</b>: push, fall through, {@code cancel()} closes it later.
   *   <li><b>Cancel arrived before our push</b>: we still see our entry, close it here.
   *   <li><b>Cancel drain took it first</b>: {@code remove} returns false, nothing to do.
   * </ol>
   */
  public AutoCloseable registerCloseOnCancel(AutoCloseable resource) {
    cancellationHandles.push(resource);
    if (cancelled.get() && cancellationHandles.remove(resource)) {
      closeQuietly(resource);
      return () -> {};
    }
    return () -> cancellationHandles.remove(resource);
  }

  /** Idempotent cancel: flips the flag and closes any registered handles in LIFO order. */
  public void cancel() {
    if (!cancelled.compareAndSet(false, true)) return;
    AutoCloseable h;
    while ((h = cancellationHandles.pollFirst()) != null) {
      closeQuietly(h);
    }
  }

  private void closeQuietly(AutoCloseable h) {
    try {
      h.close();
    } catch (Exception e) {
      LOG.warn("Cancellation handle close failed (sessionId={})", sessionId, e);
    }
  }
}
