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

/**
 * Typed signal that an agent run was cancelled via {@link AgentRunContext#cancel()}. Thrown at
 * cancellation checkpoints ({@link AgentRunContext#throwIfCancelled()}) or translated from the
 * underlying exception a blocking resource surfaces when cancel closes it. The agent loop catches
 * this exactly once and maps it to an {@code AgentCancelled} event, so callers no longer need to
 * poll {@link AgentRunContext#isCancelled()} at every checkpoint.
 */
public class AgentCancelledException extends RuntimeException {

  public AgentCancelledException() {
    super("Agent run cancelled");
  }

  public AgentCancelledException(Throwable cause) {
    super("Agent run cancelled", cause);
  }
}
