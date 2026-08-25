# LangChain v1 是怎么做 Stop 的？—— 与 Kyuubi Data Agent 的对比

> 阅读对象：Kyuubi Data Agent 的贡献者、reviewer
> 目的：借鉴主流框架的取消模型，为 Kyuubi 后续的 Stop 重构提供参考
> 结论一句话：**LangChain v1 通过 LangGraph 的图运行时统一处理"取消 / 跳转 / 中断"，不需要业务代码在 for 循环里手写 checkpoint。**

---

## 一、LangChain v1 的架构底座

`create_agent()` 返回一个 **LangGraph 的 `CompiledStateGraph`**（不是 Python 的 for 循环）。Agent 的"每一步"都是图中的一个 **node**：

```
START ─▶ before_agent ─▶ before_model ─▶ model ─▶ after_model ─▶ tools ─▶ ... ─▶ END
                            ▲                                        │
                            └────────────────────────────────────────┘
```

图运行时（LangGraph）负责调度、检查点、状态管理、流式输出。**取消能力是运行时天然提供的**，业务代码只需要"发出停止意图"，不需要"确保循环真的退出"。

---

## 二、LangChain v1 停止机制的三层设计

### 层 1：**声明式跳转 `jump_to`**（业务发起的软停止）

任何中间件都可以在 `before_model` / `after_model` 等 hook 里返回：

```python
return {"jump_to": "end", "messages": [limit_ai_message]}
```

配合装饰器声明"我这个 hook 可以跳到哪里"：

```python
@hook_config(can_jump_to=["end"])
def before_model(self, state, runtime): ...
```

然后 factory.py 的调度中枢在**每次节点转移前**读一遍 `state["jump_to"]`：

```python
# libs/langchain_v1/langchain/agents/factory.py
if jump_to := state.get("jump_to"):
    return _resolve_jump(jump_to, ...)
```

**语义**：这是一个**协作式停止**。中间件把"我要终止"写进 state，图运行时下一次调度时看到就跳到 END。就像 Kyuubi Data Agent 的 `Decision.abort`，但不需要 for 循环手动检查。

**LangChain 内置用例**：

| 中间件 | 触发条件 | jump_to |
|---|---|---|
| `ModelCallLimitMiddleware` | 达到 `thread_limit` / `run_limit` | `end`（对应 kyuubi 的 `maxIterations`）|
| `ToolCallLimitMiddleware` | 达到工具调用次数上限 | `end` |

### 层 2：**运行时可持久化中断 `interrupt()`**（HITL 类型的硬停止）

Human-in-the-loop 场景（≈ Kyuubi 的 `ApprovalMiddleware`）用的是 LangGraph 提供的 `interrupt()`：

```python
# libs/langchain_v1/langchain/agents/middleware/human_in_the_loop.py
from langgraph.types import interrupt

decisions = interrupt(hitl_request)["decisions"]
```

**这是一个魔法函数**：调用它时图运行时会：

1. 把当前图状态**持久化到 checkpointer**（数据库/内存）
2. 把当前 Python 调用栈**弹出**（不是阻塞、不是 `future.get`）
3. 向外部返回中断事件

外部（另一个进程 / 一次新的 HTTP 请求）通过 `graph.invoke(Command(resume=...))` 恢复运行时，图从上次中断的位置继续。

**语义**：**这不是阻塞式等待**，是"存档 → 退出 → 恢复"三段式。因此天然支持"用户 24 小时后再回来审批"这种场景，也天然不会有 Kyuubi 现在遇到的"Approve 迟到导致危险 SQL 被执行"问题——因为如果 session 已 close，checkpoint 就废了，`Command(resume=...)` 找不到断点就直接失败。

### 层 3：**Python asyncio 的 `CancelledError`**（真正的强制取消）

LangChain 的核心调用协议 `Runnable`：

- 同步方法：`invoke()` / `stream()`
- 异步方法：`ainvoke()` / `astream()` / `astream_events()`

外部 stop 的实现依赖 asyncio 原生能力：

```python
task = asyncio.create_task(agent.ainvoke(input))
# user clicks Stop:
task.cancel()
```

`task.cancel()` 会向协程内注入 `asyncio.CancelledError`，正在 `await` 的地方（LLM HTTP 请求、tool 执行、`asyncio.sleep`）**立刻抛异常**。这是**语言级抢占**，不需要业务代码检查 flag。

Anthropic / OpenAI SDK 的 `AsyncStream` 都实现了 `__aexit__` 协议：cancel 时会自动关闭底层 HTTPX 连接，SSE 立即断开、上游 provider 收到 client disconnect、token 计费停止。

---

## 三、三层机制的分工

```
┌─────────────────────────────────────────────────────────┐
│ 用户操作          语义      落地机制                     │
├─────────────────────────────────────────────────────────┤
│ 中间件说"够了"    软停止    jump_to = "end"              │
│ 需要人来审批      暂停      interrupt() + checkpointer   │
│ 用户点 Stop       硬取消    task.cancel() + CancelledError│
└─────────────────────────────────────────────────────────┘
```

三层是**正交**的，可以叠加使用。关键设计：

1. **不在业务代码里手写 checkpoint**。业务只声明"我要停"或者"这里需要审批"，剩下由运行时（LangGraph + asyncio）保证信号能穿透每一层。
2. **停止即取消底层 IO**。asyncio 的 cancel 直达 HTTPX 底层，不存在"前端断了后端还在烧 token"的问题。
3. **审批状态可持久化**。用 checkpointer 替代 `CompletableFuture.get(300s)`，从根本上避免了"pending future 泄漏"和"Session 关闭后 resolve 迟到"这两类 bug。

---

## 四、对比 Kyuubi Data Agent 当前实现

| 维度 | LangChain v1 | Kyuubi Data Agent（当前 PR 后）|
|---|---|---|
| 主循环形态 | LangGraph 状态图 | Java `for (int step = 1; step <= maxIterations; step++)` |
| 迭代次数上限 | `ModelCallLimitMiddleware` 声明 `jump_to="end"` | for 循环 `maxIterations` + 到顶抛 `AgentError` |
| 业务停止 | Middleware 返回 `{"jump_to": "end", "messages": [...]}` | Middleware 返回 `Decision.abort(reason)` |
| 迭代空档取消 | 图运行时天然检查 `jump_to` | 手写 `if (ctx.isCancelled()) return;` checkpoint |
| LLM 流中取消 | asyncio `task.cancel()` → HTTPX 抢占 | `ctx.onCancel(stream::close)` 手动接线 |
| 人机审批阻塞 | `interrupt()` 持久化，非阻塞 | `CompletableFuture.get(300s)` 线程阻塞 |
| 迟到 Approve 的安全性 | checkpoint 已废，`Command(resume=)` 直接失败 | `pending.remove(id)`，remove-then-complete no-op（本次 PR 已修）|
| 工具执行中取消 | tool 是 `async` 函数，`await` 处自动响应 cancel | `future.join()` 前检查 `isCancelled()` + 工具级 timeout |
| 上下游断开 | HTTPX 层自动断连 | `LlmStreamClient` 显式调 `stream.close()` |
| 引擎级 stop | `graph.astream()` 的外层 `task.cancel()` | `ReactAgent.stop()` 遍历 `activeRuns.values().forEach(cancel)` |

---

## 五、启示与借鉴价值

### 短期（本次 PR 已经做的）
本次 Kyuubi PR 已经把 LangChain 的**思路 1（jump_to）** 手写成了 Java 版：

- `AgentRunContext.cancel()` ≈ `state["jump_to"] = "end"`
- `checkCancelled(ctx)` ≈ 图运行时的调度检查点
- `AgentCancelledException` ≈ LangGraph 的 `GraphInterrupt`
- `ctx.onCancel(stream::close)` ≈ asyncio 的 CancelledError 传播路径

区别在于 LangChain 由**运行时框架**保证，Kyuubi 由**业务代码**保证。当前实现已经能覆盖 Issue 中列出的所有 Stop 场景。

### 中长期（未来可选演进）
如果 Kyuubi Data Agent 后续想进一步靠近主流架构，有两个演进方向：

1. **审批用可持久化的方式重写**
   - 现状：`ApprovalMiddleware` 用 `CompletableFuture` + 300s timeout，agent 线程被占住
   - 目标：审批时 Agent **保存状态并退出**，`ExecuteStatement` 变成 idempotent 的"给定 session + step 从存档恢复"
   - 好处：Agent 线程不再阻塞、支持长审批（跨天）、天然避免"迟到 Approve 授权已取消操作"

2. **考虑接入 async / reactive 编程模型**
   - 现状：Java 阻塞式，`stream.forEach` + `future.get` 混用，取消要靠外部 close 触发
   - 目标：底层用 Reactor/CompletableFuture 组合，取消由 subscription 传播
   - 好处：与 asyncio `CancelledError` 类似的抢占语义，不再需要 checkpoint

> 这两条都不是本次 Stop 修复的目标，属于架构演进候选，仅作为背景信息记录。

---

## 六、可作 PR 描述引用的原文位置（LangChain v1）

| 机制 | 文件 |
|---|---|
| `jump_to` 调度中枢 | `libs/langchain_v1/langchain/agents/factory.py`（`_resolve_jump` / `state.get("jump_to")`）|
| `@hook_config(can_jump_to=[...])` 声明 | `libs/langchain_v1/langchain/agents/middleware/model_call_limit.py`（`ModelCallLimitMiddleware.before_model`）|
| `interrupt()` HITL | `libs/langchain_v1/langchain/agents/middleware/human_in_the_loop.py`（`from langgraph.types import interrupt`）|
| Tool 调用次数限制 | `libs/langchain_v1/langchain/agents/middleware/tool_call_limit.py` |

上游 langgraph 仓库（`langchain-ai/langgraph`）中的 `Command` / `interrupt` / `GraphInterrupt` 才是真正的运行时实现，本仓库内可见的只是使用方。
