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

- 提交任务（`Pool.Submit`），返回 job ID
- 使用带缓冲 channel 作为任务队列
- 使用固定数量 worker 构成 worker pool
- 任务状态流转：
  - `pending`
  - `running`
  - `success`
  - `failed`
- 失败自动重试，最多重试 `MaxRetries` 次
- 通过 `Executor` 接口模拟不同任务执行结果（成功 / 失败 / 多次重试后成功）
- 通过 `Store` 接口隔离任务存储，当前实现是内存版 `MemStore`
- 使用 `sync.RWMutex` 保证任务状态读写的并发安全
- 使用 `sync.WaitGroup` 实现 graceful shutdown：等所有任务落地 → 关闭 channel → 等所有 worker 退出

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
│   └── worker
│       └── pool.go              # Pool 类型（worker 池 + 调度 + 重试）
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

带数据竞争检测器：

```bash
go run -race ./cmd/taskflow
```

预期输出（worker 编号、UUID 每次不同）：

```
worker - 0 begin job 5a10ab49-...
worker - 1 begin job cf30f1e2-...
worker - 2 begin job e450d208-...
worker - 1 success job cf30f1e2-...
...
job Status success
job Status failed
job Status success
```

三个 job 分别演示：

| Payload   | 行为                                     | 最终状态 |
| --------- | ---------------------------------------- | -------- |
| `success` | 一次成功                                  | `success` |
| `fail`    | 每次都失败，重试到上限后放弃              | `failed`  |
| `flaky`   | 前 `MaxRetries-1` 次失败，最后一次成功    | `success` |

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

```go
func (p *Pool) Shutdown() {
    p.jobWg.Wait()      // 1. 等所有 in-flight job（包括 retry）到达终态
    close(p.queue)      // 2. 关闭 channel，让 worker 的 for-range 退出
    p.workerWg.Wait()   // 3. 等所有 worker goroutine 结束
}
```

`jobWg` 数"还在飞的 job 数"，`workerWg` 数"worker goroutine 数"。两个 WaitGroup 分别负责不同的生命周期事件——这是 worker pool graceful shutdown 的通用模板。

### 4. retry 不重复计数

job 在重试时**不调用 `jobWg.Done()`**——因为它还 in-flight。只有到达终态（Success / 最终 Failed）才 `Done`。这样 `jobWg.Wait()` 能正确等到所有 retry 跑完。

## 已知问题 / 待改进

这是个学习中的项目，已经识别出但还没动手的点：

- [ ] 没有 `context.Context` 支持，无法外部触发取消
- [ ] 没有结构化日志，目前用 `fmt.Printf`
- [ ] 没有监听 SIGINT/SIGTERM 信号优雅退出
- [ ] `MemStore` 只是内存版，重启数据丢失
- [ ] 没有单元测试
- [ ] `Pool.HandleFail` 应该是私有方法（`handleFail`）
- [ ] 任务执行 `time.Sleep(1s)` 是写死的模拟时间，应该让 executor 自己控制
- [ ] 没有 metrics / 可观测性

## 依赖

- `github.com/google/uuid` —— 生成任务 ID

## License

学习用，无 License。