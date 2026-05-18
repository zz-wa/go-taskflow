# go-taskflow

> 还在继续ing

go-taskflow 是一个用 Go 实现的轻量级异步任务队列 demo。

这个项目不是要实现一个生产级消息队列，而是通过一个尽量小的任务系统，练习后端开发中常见的异步处理模型，包括 worker pool、channel、任务状态流转、失败重试、并发安全和优雅退出。

## 项目背景

在后端系统中，有些任务不适合在请求链路里同步完成，例如：

- 发送邮件
- 推送通知
- 生成报表
- 图片处理
- 订单超时取消
- 调用不稳定的第三方接口

这类任务通常会被提交到后台队列中，由 worker 异步消费。go-taskflow 就是对这个模型的一个最小实现。

## 当前功能

- 通过 HTTP 提交任务，返回 job ID
- 通过 HTTP 查询任务状态
- 提供健康检查接口
- 提交任务（`Pool.Submit`），返回 job ID；队列满时返回错误
- 使用带缓冲 channel 作为任务队列
- 使用固定数量 worker 构成 worker pool
- 支持单次 job 执行超时控制（`JobTimeout`）
- 任务状态流转：
  - `pending`
  - `running`
  - `success`
  - `failed`
- 失败自动重试，最多重试 `MaxRetries` 次
- retry 重新入队时使用非阻塞写入，队列满则直接标记任务失败，避免 worker 卡死
- 超时任务复用统一失败处理流程：执行失败 → retry → 超过重试上限后 failed
- 通过 `Executor` 接口模拟不同任务执行结果（成功 / 失败 / 多次重试后成功 / 超时）
- `Executor.Execute` 接收 `context.Context`，可以响应 timeout / cancel
- 通过 `Store` 接口隔离任务存储，当前实现是内存版 `MemStore`
- 使用 `sync.RWMutex` 保证任务状态读写的并发安全
- 使用 `sync.WaitGroup` 实现 graceful shutdown：停止 HTTP 服务 → 等所有任务落地 → 关闭 channel → 等所有 worker 退出
- 使用表驱动测试覆盖 success / fail / flaky / timeout 四种执行路径，并覆盖 HTTP health 与 retry 队列满场景

## 项目结构

```text
go-taskflow
├── cmd
│   └── taskflow
│       └── main.go              # 入口，负责 wiring（构造 store / pool / executor 并启动）
├── internal
│   ├── executor
│   │   └── executor.go          # Executor 接口 + 默认实现（业务执行逻辑）
│   ├── job
│   │   ├── job.go               # Job 结构体 + 状态常量
│   │   └── store.go             # Store 接口 + MemStore 内存实现
│   ├── transport
│   │   └── http
│   │       └── server
│   │           ├── router.go     # HTTP 路由
│   │           ├── server.go     # HTTP handler
│   │           └── server_test.go
│   └── worker
│       ├── pool.go              # Pool 类型（worker 池 + 调度 + 重试 + timeout）
│       └── pool_test.go         # worker pool 表驱动测试
├── go.mod
└── go.sum
```

为什么用 `cmd/` + `internal/` 这种布局：

- `cmd/<name>/` 是 Go 社区惯例的可执行入口位置，一个 repo 可以放多个 binary
- `internal/` 是 Go 编译器强制的私有目录，外部 module 不能 import，作为"包私有"的语言级保证

## 运行

```bash
go run ./cmd/taskflow
```

服务默认监听：

```text
http://localhost:8080
```

带数据竞争检测器：

```bash
go run -race ./cmd/taskflow
```

### HTTP API

健康检查：

```bash
curl http://localhost:8080/health
```

响应：

```json
{"status":"ok"}
```

提交任务：

```bash
curl -X POST http://localhost:8080/submit \
  -H 'Content-Type: application/json' \
  -d '{"jobtype":"test","payload":"success"}'
```

响应状态码为 `202 Accepted`，表示任务已进入队列等待异步执行：

```json
{"id":"<job-id>","status":"pending"}
```

查询任务：

```bash
curl http://localhost:8080/jobs/<job-id>
```

响应示例：

```json
{
  "id": "<job-id>",
  "jobtype": "test",
  "payload": "success",
  "status": "success",
  "retryTimes": 0,
  "maxRetries": 3
}
```

队列满时，提交接口返回 `503 Service Unavailable`：

```json
{"error":"queue is full"}
```

当前 `executor.Default` 用 `payload` 模拟不同任务行为：

| Payload   | 行为                                     | 最终状态 |
| --------- | ---------------------------------------- | -------- |
| `success` | 一次成功                                  | `success` |
| `fail`    | 每次都失败，重试到上限后放弃              | `failed`  |
| `flaky`   | 前 `MaxRetries-1` 次失败，最后一次成功    | `success` |
| `timeout` | 每次执行都超过 `JobTimeout`，最终失败      | `failed`  |

按 `Ctrl+C` 退出服务时，程序会先关闭 HTTP server，不再接收新请求，然后等待队列中已有任务处理完成，最后关闭 worker pool。

## 测试

运行全部测试：

```bash
go test -v -count=1 -timeout 10s ./...
```

运行 race detector：

```bash
go test -race -count=1 -timeout 10s ./...
```

如果本地遇到 Go build cache 不可写，可以临时指定 `GOCACHE`：

```bash
env GOCACHE=/tmp/go-build go test -v -count=1 -timeout 10s ./...
env GOCACHE=/tmp/go-build go test -race -count=1 -timeout 10s ./...
```

## 关键设计点

### 1. 通过接口解耦：worker 不知道 store 和 executor 的具体实现

`Pool` 依赖的是 `job.Store` 和 `executor.Executor` 两个接口：

```go
func New(cfg Config, exec executor.Executor, store job.Store) *Pool
```

以后想换 Redis store 或 HTTP executor，pool 一行不用改。

### 2. 没有包级全局变量

`WaitGroup` / `mutex` / `channel` / `map` 全部封装在 `Pool` / `MemStore` 实例字段里，不再是包级 `var`。整个程序可以同时跑多个 `Pool` 实例互不干扰，也更好测试。

### 3. Graceful shutdown 的两阶段等待

`main.go` 先处理进程信号和 HTTP server 生命周期：

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := httpServer.Shutdown(ctx); err != nil {
    log.Fatal(err)
}
pool.Shutdown()
```

`Pool.Shutdown` 再处理任务队列和 worker 生命周期：

```go
func (p *Pool) Shutdown() {
    p.jobWg.Wait()      // 1. 等所有 in-flight job（包括 retry）到达终态
    close(p.queue)      // 2. 关闭 channel，让 worker 的 for-range 退出
    p.workerWg.Wait()   // 3. 等所有 worker goroutine 结束
}
```

`jobWg` 数"还在飞的 job 数"，`workerWg` 数"worker goroutine 数"。两个 WaitGroup 分别负责不同的生命周期事件——这是 worker pool graceful shutdown 的通用模板。

### 4. 队列满时快速失败

`Pool.Submit` 使用非阻塞 channel 写入：

```go
select {
case p.queue <- j:
    return j.ID, nil
default:
    p.jobWg.Done()
    p.store.Delete(j.ID)
    return "", ErrQueueFull
}
```

这样 HTTP 请求不会因为队列满而一直挂住，而是直接返回 `503`。

retry 重新入队时也使用非阻塞写入。如果队列满，任务会被标记为 `failed` 并结束，避免所有 worker 都阻塞在重试入队上。

### 5. retry 不重复计数

job 在重试时**不调用 `jobWg.Done()`**——因为它还 in-flight。只有到达终态（Success / 最终 Failed / retry 队列满）才 `Done`。这样 `jobWg.Wait()` 能正确等到所有 retry 跑完。

### 6. 每次 job 执行都有独立 timeout

`worker` 不负责模拟任务耗时，只负责给每次执行创建带超时的 `context`：

```go
ctx, cancel := context.WithTimeout(context.Background(), p.cfg.JobTimeout)
err := p.exec.Execute(ctx, j)
cancel()
```

真正的任务行为放在 `executor` 里。比如 `timeout` payload 会等待较长时间，但同时监听 `ctx.Done()`：

```go
select {
case <-time.After(5 * time.Second):
    return nil
case <-ctx.Done():
    return ctx.Err()
}
```

这样超时和普通失败都能走同一套 `HandleFail` / retry / failed 流程。

## 已知问题 / 待改进

这是个学习中的项目，已经识别出但还没动手的点：

- [ ] 没有结构化日志，目前用 `fmt.Printf`
- [ ] `MemStore` 只是内存版，重启数据丢失
- [ ] `Pool.HandleFail` 应该是私有方法（`handleFail`）
- [ ] 还没有外部取消整个 Pool 的机制，目前只支持单次 job timeout
- [ ] HTTP handler 测试还可以继续补齐 submit / get / queue full 等路径
- [ ] 没有 metrics / 可观测性

## 依赖

- `github.com/google/uuid` —— 生成任务 ID

## License

学习用，无 License。
