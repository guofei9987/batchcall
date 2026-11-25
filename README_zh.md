# BatchCall

轻量级 Python 批量调用框架，支持独立自适应并发、按轮次重试、JSONL 持久化与断点续跑。适用于 LLM、Agent、工作流等 I/O 密集型任务。Python 3.9+，无第三方运行时依赖。

[English](README.md)

## 安装与使用

首个版本发布后，可通过 PyPI 安装：

```bash
python -m pip install batchcall
```

也可以在项目目录运行 `python -m pip install .`。

```python
from batchcall import run, StorageConfig, RetryConfig, LogConfig

class MyCaller:
    concurrency = 10
    adaptive = True
    max_concurrency = 50

    def __call__(self, input_data):
        # 替换为自己的线程安全业务调用
        return {"status": "success", "output": input_data}

caller1 = MyCaller()
caller2 = MyCaller()
caller2.concurrency = 20
caller2.max_concurrency = 100

data = {"id_001": '{"query":"问题1"}', "id_002": '{"query":"问题2"}'}
summary = run(data, [caller1, caller2],
    storage_config=StorageConfig(output_path="experiment.jsonl"),
    retry_config=RetryConfig(max_retries=3),
    log_config=LogConfig(log_path="runtime.log"))
```

也可以直接 `run(data, caller1)`。Caller 无需继承任何基类。Token、API Key、超时和线程安全均由用户管理；各 Caller 必须能够处理同一批任务。

## Caller 接口

输入为 JSON 字符串，返回 dict：`status`（必填，`success`、`rate_limited` 或自定义失败状态）、`output`（必填，JSON 字符串，失败时可为 None）、`duration_s`（可选，秒，不提供则自动测量）、`error`（可选，供人排查）。普通异常及不合规返回转换为 `exception`。框架不解析业务 JSON；用户需要自行识别 429 并返回 `rate_limited`。

Caller 可选字段：`name`（自动生成）、`concurrency=10`、`adaptive=False`、`max_concurrency=50`、`adjust_every_n=100`。每个 Caller 独立统计已完成调用，429 比例 ≤1% 时并发 +1，>5% 时减半向下取整，其余保持。并发最低 1，最高为指定上限。不取消在途调用，不修改 Caller 静态配置。RPM 仅监控，不主动限流。

## 全局配置

| 配置 | 默认参数 |
| --- | --- |
| StorageConfig | output_path="results.jsonl", save_every_n=50, resume=True |
| RetryConfig | max_retries=3（主体任务之外的重试轮次） |
| LogConfig | log_every_n=100, console=True, log_path=None |

每次调用（成功或失败）均追加一条 JSONL 记录，包含 `id`、`input`、`output`（业务结果字符串，失败时可为 null）、`status`（调用状态）、`elapsed`（耗时，秒）、`error_msg`、`caller_name`、`round`，均为顶层字段。每 N 次完成调用批量写入并 flush；每轮结束及退出时尽可能保存剩余结果。

示例：

```json
{"id":"id_001","input":"{\"query\":\"hello\"}","output":"{\"answer\":\"hi\"}","status":"success","elapsed":0.123,"error_msg":null,"caller_name":"caller_0","round":0}
```

**格式变更：** 不再支持旧版嵌套的 `output.response`、`output.status`、`output.elapsed` 结构。读取或断点续跑前，请使用新文件或先迁移旧记录；即使 `resume=False` 也不允许混写。Caller 接口保持不变。

启动时流式扫描历史文件，只保留至少成功一次的 ID。完整但未换行的尾部记录自动补换行；非法且未换行的末行截断；其他损坏记录报错。主体全部执行后，上一轮失败 ID 进入下一轮。重试次数仅在单次运行内有效。`resume=False` 仍追加历史记录。

```python
from batchcall import load_success_ids, iter_results
ids = load_success_ids("experiment.jsonl")
for record in iter_results("experiment.jsonl"):
    if record['status'] == 'success':
        pass  # 处理成功记录
```

`iter_results` 逐条返回所有尝试，不自动去重。`run` 返回运行摘要，不在内存累计业务结果。默认日志包含全局及 Caller RPM、并发、在途请求、进度、成功失败数和持久化数。

注意：同一个文件中的 ID 必须始终对应相同实验输入；不同实验使用不同文件。不能多个进程或运行同时写同一文件。异常退出可能重复调用，不保证 exactly-once；有副作用的调用应具备幂等性。日志输出到 stderr，可选本地日志。请注意结果文件中的敏感信息。

测试：`python -m unittest discover -s tests -v`（先安装包）。MIT License。
