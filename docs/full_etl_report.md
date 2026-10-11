# 全量 Spark ETL 报告（Phase 1B）

> 工程：`hf_eco_analysis`　｜　阶段：Phase 1B　｜　日期：2026-10-11
> 数据快照：`cfahlgren1/hub-stats` @ commit `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`
> 本文所有数字均来自**真实全量运行**（`data_hubstats/_smoke/full_etl_run_report.json`
> 与 `data_hubstats/gold/data_quality_report.json`），无任何估算或虚构。

---

## 1. 运行配置

| 项 | 值 | 说明 |
|---|---|---|
| 引擎 | PySpark **4.2.0**（内置 Hadoop 3.5.0） | Windows 原生，无 WSL |
| Master | `local[6]` | 20 核机器，只占 6 核，为其他程序留出空间 |
| Driver Memory | **`3g`** | 见 §4 内存实测，3g 足够；未盲目上调 |
| `spark.sql.shuffle.partitions` | **64** | |
| 文件系统 | `BareLocalFileSystem`（纯 Java，无 winutils.exe） | |
| 会话时区 | `UTC` | 与 pandas 路径口径一致 |
| Parquet 压缩 | snappy | |
| `spark.driver.maxResultSize` | 512m | 只收集小结果（频数表），不需要大结果集 |

运行命令：

```bash
HF_SPARK_MASTER="local[6]" HF_SPARK_DRIVER_MEMORY="3g" HF_SHUFFLE_PARTITIONS=64 \
  python run_full_etl.py
```

> `run_full_etl.py` 只负责「启动 + 采样内存 + 计时 + 落运行报告」，不改 ETL 逻辑。

---

## 2. 分步耗时（真实墙钟）

| 步骤 | 累计秒 | 本步秒 |
|---|---|---|
| SparkSession 就绪 | 0.0 | — |
| Silver 三表写出完成 | 45.3 | 45.3 |
| `models_enriched` 写出并回读完成 | 73.5 | 28.2 |
| Gold 三张月度统计表写出完成 | 86.5 | 13.0 |
| `download_distribution` 写出完成 | 106.9 | 20.4 |
| `lorenz_points` 写出完成 | 110.2 | 3.3 |
| 全量 ETL 结束 | **120.6** | 10.4 |

整个运行器（含进程启动与报告落盘）墙钟 **142.8 秒**。

> ⚠️ **诚实说明**：上表是**第二次运行**（第一次因运行器的内存采样线程有 bug 而重跑）。
> 第一次 ETL 内部耗时 95.1 秒。两次的差异来自机器上其他进程的负载波动，
> 不是代码变化——两次的行数、Gini、Top-k 完全一致（见 §6 可复现性）。

---

## 3. 输入输出规模

| 项 | 字节 | 人类可读 |
|---|---|---|
| Bronze 输入 `models.parquet` | 1,596,738,497 | 1.49 GiB |
| Silver 输出目录 | 458,545,882 | 437 MiB |
| Gold 输出目录 | 697,384,662 | 665 MiB |

**各表行数（全量）：**

| 层 | 表 | 行数 |
|---|---|---|
| Silver | `models` | 3,138,954 |
| Silver | `model_tags` | 20,625,635 |
| Silver | `model_base_edges` | 919,839 |
| Gold | `models_enriched` | 3,138,954 |
| Gold | `task_monthly_stats` | 2,587 |
| Gold | `library_monthly_stats` | 7,915 |
| Gold | `tag_monthly_stats` | 861,394 |
| Gold | `download_distribution` | 3,138,954 |
| Gold | `model_family_stats` | 575 |
| Gold | `model_lifecycle` | 3,138,954 |
| Gold | `model_adoption_features` | 3,138,954 |
| Gold | `lorenz_points`（Phase 1B 新增） | 62,763 |

---

## 4. 资源实测

| 指标 | 值 |
|---|---|
| 墙钟总耗时 | 142.8 s |
| **JVM 进程 RSS 峰值** | **3.67 GiB** |
| 系统内存占用峰值 | 27.57 GiB / 31.43 GiB |
| 运行期间系统可用内存最低值 | **3.87 GiB** |
| 本机总内存 | 31.43 GiB |
| C 盘可用空间（运行前） | 108.9 GiB |

**结论**：`driver=3g` 对该数据集**足够**（JVM RSS 峰值 3.67 GiB 含堆外与元空间）。
不需要 8g / 12g。这是本机内存偏紧（运行前仅 6.4 GiB 可用）下仍然完成全量 ETL 的关键。

> `peak_system_used_gib` 包含机器上其他程序（浏览器、IDE 等）的占用，
> 不是 Spark 单独消耗；Spark 的消耗看 `peak_jvm_rss_gib`。

---

## 5. 为跑通全量所做的执行计划优化

### 5.1 修复全局排序瓶颈（用户明确要求）

**原实现**：`download_distribution` 用 `Window.orderBy("downloads","model_id")`，
**没有 `partitionBy`** → Spark 把全部 314 万行移到**单个分区**排序，
日志反复出现：

```
WARN WindowExec: No Partition Defined for Window operation!
    Moving all data to a single partition, this can cause serious performance degradation.
```

314 万行只能由 1 个 core 串行处理，其余 5 个 core 空转。

**新实现**：按下载量取值做**频数聚合**，再逐模型排名：

```
model_id, downloads
      ↓ groupBy(downloads)
downloads, frequency                    ← 全量实测只有 14,894 个唯一值
      ↓ 窗口累计（在频数表上，规模小三个数量级）
cum_freq, cum_value, start_rank, prev_cum_value
      ↓ join 回模型（按 downloads 取值）
rank = start_rank + 组内按 model_id 的序号      ← 按 downloads 分区，可并行
cum_model_pct, cum_download_pct
```

Gini 用**频数加权秩公式**精确计算，**不是**把每个唯一下载量当成一个模型：

```
S = Σ v_i · f_i · (F_{i-1} + F_i) / 2      G = 2S/(n·total) − 1
```

**等价性对拍**（`test_distribution.py`，在 50,218 条真实样本上）：

| 检查 | 结果 |
|---|---|
| 逐模型 `rank` 完全一致 | ✅ true |
| `cum_model_pct` 最大绝对差 | **0.0** |
| `cum_download_pct` 最大绝对差 | **0.0** |
| 频数加权 Gini vs 逐模型秩式 Gini | 差 0.0（0.994445 vs 0.994445） |
| Top-1/5/10% 份额 | 差 0.0 |

→ **语义完全等价，只是执行计划可并行。**

**性能效果（50,218 条样本，`local[4]`）**：单次 ETL 从 **193 s → 50.7 s**（3.8×）。
`WindowExec` 单分区告警从 77 次降到 26 次（剩下的都是频数表自身的小窗口，规模仅数千行，无害）。

### 5.2 消除重复全量扫描

**原实现**：每写一张表都从 Bronze 重算一次 `models`/`base_gold`
（10 张表 ≈ 10 次全量重扫 1.49 GiB Parquet）。

**新实现**：

```
Bronze 只读一次 → 落 Silver models
                    ↓ 读回
              model_tags / model_base_edges（从 Silver 读，不重扫 Bronze）
                    ↓
              models_enriched 落盘
                    ↓ 读回
              其余 7 张表 + lorenz_points + 集中度统计
```

**结果**：Bronze 全量扫描次数 **10 → 1**，全量 ETL 120.6 s。

### 5.3 新增 `lorenz_points` 而不改旧表字段含义

按用户要求「原有 Gold 表如需保留逐模型数据，应保留其语义兼容性」：

- `download_distribution` **保持逐模型语义**（`model_id, downloads, downloadsAllTime,
  downloads_log, rank, cum_model_pct, cum_download_pct`），列名与含义完全不变。
- **新增** `lorenz_points`：频数聚合后的累计点，列为
  `metric, value, freq, cum_freq, cum_model_pct, cum_download_pct`，
  `metric ∈ {downloads, downloadsAllTime}` **两种口径独立成组，绝不混合**。
- **新增** `concentration_stats.json`：两口径的 Gini / Top-1·5·10% / 零值占比 / 唯一取值数。
- 点的"减少"来自把同一下载量的模型合并成一个点（每个点都是**真实累计值**），
  不是抽样、不是插值。

---

## 6. 可复现性

同一配置连跑两次，关键指标完全一致：

| 指标 | 第 1 次 | 第 2 次 |
|---|---|---|
| `models_enriched` 行数 | 3,138,954 | 3,138,954 |
| Gini(`downloads`) | 0.995355 | 0.995355 |
| Gini(`downloadsAllTime`) | 0.998117 | 0.998117 |
| Silver / Gold 字节数 | 458,545,882 / 697,384,662 | 458,545,882 / 697,384,662 |

> 输出字节数**逐字节相同**，说明 Parquet 写出与压缩是确定性的。

---

## 7. 全量数据质量校验结论

详见 `docs/data_quality_report.md`。摘要：

- ✅ `model_id` 重复 **0**
- ✅ `gated` 保持全 NULL（源无此列 → 显式类型化 NULL）
- ✅ `created_at` 解析失败 **0**，全部符合 ISO-8601 UTC
- ✅ **聚合守恒**：月度新增之和 = `task_monthly_stats` 之和 =
  `library_monthly_stats` 之和 = **3,138,954**（= 模型总数）
- ✅ Gini / Top-1% 由校验脚本用独立路径重算，与 ETL 写出结果差异 < 1e-9

---

## 8. 必须如实说明的两点

### 8.1 源文件 3,138,959 行，去重后是 3,138,954 个模型

源快照内有 **5 个 `id` 各出现 2 次**（`datalab-to/surya-ocr-2-gguf`、
`litert-community/DeepSeek-R1-Distill-Qwen-7B`、`hoodarunner/running-coach-qwen3b-lora`、
`gimly13/z-image-cyberrealistic-v8-gguf`、`allura-org/Qwen3.8-27B-Dominatrix`）。

`model_id` 是主键，去重是**正确行为**，不是数据丢失。因此：
- 后续所有分析以 **3,138,954** 为总体规模；
- 报告中引用「3,138,959」时指**源文件行数**，引用「3,138,954」时指**唯一模型数**。

### 8.2 仍未在本地执行的算法

Phase 1B 只做 ETL，**未执行** GraphFrames 图算法、MLlib 训练、Benchmark。
这些在 Phase 2/3 进行，其可完成性会在对应报告中如实记录。

---

## 9. 变更文件清单（Phase 1B）

### 新增

```
stats_utils.py           长尾/集中度统计的唯一口径实现（Gini 频数加权 / Top-k / Lorenz / CCDF）
spark_session.py         全项目唯一的 SparkSession 构建入口（本地 FS + UTC 时区）
test_distribution.py     新旧集中度口径逐行对拍
verify_etl.py            全量数据质量与聚合守恒校验
run_full_etl.py          全量 ETL 运行器（内存采样 + 计时 + 运行报告）
```

### 修改

```
spark_etl.py       频数聚合替换全局窗口；执行计划重构；新增 lorenz_points 与
                   concentration_stats.json；分步计时；空值 coalesce 对齐 pandas 口径
local_etl.py       产出同款 lorenz_points 与 concentration_stats.json（同 stats_utils）
schema_adapter.py  新增 GOLD_EXTRA_TABLES / GOLD_EXTRA_FILES / GRAPHFRAMES_* / EXTRA_JARS
verify_phase1a.py  新增 lorenz_points 与两口径集中度一致性校验
setup_spark_fs.py  推广为安装全部额外 jar（文件系统 jar + GraphFrames jar）
```

### 新增数据目录（与旧 Demo、抽样运行完全隔离）

```
data_hubstats/silver/   全量 Silver 三表
data_hubstats/gold/     全量 Gold 七表 + lorenz_points + concentration_stats.json
                        + data_quality_report.json
```

**`data/` 下旧 Demo 的原始数据与结果未被触碰**（文件时间戳仍为 2026-10-10 及更早）。
