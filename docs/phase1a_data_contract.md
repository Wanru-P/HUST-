# Phase 1A 交付文档（二）：数据契约（Data Contract）

> 工程：`hf_eco_analysis`　｜　阶段：Phase 1A　｜　日期：2026-10-11
> 本文定义**Bronze / Silver / Gold 的字段契约、路径约定与抽样规则**，
> 是 `spark_etl.py`（Spark 路径）与 `local_etl.py`（pandas 路径）共同遵守的唯一标准。
> 所有 Schema 与缺失率均来自**真实文件读取**（Parquet row-group 元数据），非估算。

---

## 1. 数据源与固定版本

| 项 | 值 |
|---|---|
| 数据集 | `cfahlgren1/hub-stats` |
| 文件 | `models.parquet` |
| 下载通道 | `hf-mirror.com`（官方 `huggingface.co` 本机不可达） |
| **固定 commit** | `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70` |
| 完整 URL | `https://hf-mirror.com/datasets/cfahlgren1/hub-stats/resolve/2c1e195090bbcbe1f940cfdcb4ac25b76e48da70/models.parquet` |
| 字节数 | `1,596,738,497`（约 1.49 GiB） |
| **SHA-256（全文件）** | `d86485a02a7aace1b8c4eda4a849dd153a702d5351ab5a57f3c2320176e251ef` |
| 校验结果 | ✅ 与可信元数据**完整匹配**（非前缀比对） |
| 校验时间 | `2026-10-11T03:12:42Z` |
| 本地路径 | `data_hubstats/bronze/models.parquet` |
| 采集记录 | `data_hubstats/bronze/ingest_manifest.json` |

**实际行数（从 Parquet 元数据读取，非扫描估算）：**

```
num_rows       = 3,138,959
num_columns    = 69          （leaf 列数；顶层字段 20 个）
num_row_groups = 63
created_by     = parquet-cpp-arrow version 26.0.0
```

> ⚠️ **排序陷阱（Phase 0 已发现，此处再次确认）**：行组 0 是 trending 样本，
> 行组 1–62 按 `createdAt` 升序。**任何 `head(N)` / `LIMIT N` 都会得到有偏样本**，
> 本工程全流程禁止用前 N 行当抽样。

---

## 2. 全量真实 Parquet Schema（20 个顶层字段）

```
_id                          large_string
id                           large_string
author                       large_string
cardData                     large_string
inferenceProviderMapping     list<element: struct<_id, provider, providerId, status, task,
                                   features, performance, providerDetails, previousStatus,
                                   type, isModelAuthor, adapter, adapterWeightsPath>>
lastModified                 timestamp[us]          ← 时间戳类型
likes                        int64
trendingScore                double
config                       large_string
downloads                    int64                  ← 与 downloadsAllTime 不同
downloadsAllTime             int64
safetensors                  struct<parameters: struct<27 种 dtype: int64>, total: int64>
tags                         list<element: string>
pipeline_tag                 large_string
library_name                 large_string
transformersInfo             struct<auto_model, pipeline_tag, processor, custom_class>
siblings                     list<element: struct<rfilename: string>>
createdAt                    timestamp[us]          ← 时间戳类型
baseModels                   struct<relation: string,
                                       models: list<element: struct<_id: string, id: string>>>
gguf                         large_string
```

**关键确认：**
- `createdAt` / `lastModified` 是 **`timestamp[us]`**（不是字符串）→ 需要转成统一 ISO-8601 UTC 字符串。
- `baseModels` 是 **struct**：`relation` 是同级字符串，模型 id 在 `models[]` 数组的**嵌套 struct** 里
  → 必须 `transform(baseModels.models, x -> x.id)`，不能直接取列。
- **源中不存在 `gated` 字段** → Silver 必须产出**显式类型化 NULL**（`StringType`），
  而不是省略该列（否则下游 RQ 模块按名取列会崩）。
- 源中共 69 个 leaf 列，本工程按契约**只取 11 个**（见 §4），其余保留在 Bronze 不丢弃。

---

## 3. 全量关键字段缺失率（3,138,959 行，来自 row-group 统计）

### 3.1 契约用到的字段

| 字段 | null 数 | 缺失率 | 说明 |
|---|---|---|---|
| `id` | 0 | **0.000000** | 主键，无缺失 |
| `author` | 0 | **0.000000** | 无缺失 |
| `createdAt` | 0 | **0.000000** | 无缺失 |
| `lastModified` | 0 | **0.000000** | 无缺失 |
| `downloads` | 0 | **0.000000** | 无缺失 |
| `downloadsAllTime` | 0 | **0.000000** | 无缺失 |
| `likes` | 0 | **0.000000** | 无缺失 |
| `tags.list.element` | 0 | **0.000000** | 元素级无 null |
| `pipeline_tag` | 2,057,743 | **0.655549** | **缺失率最高，直接影响 RQ5 任务生态分析** |
| `library_name` | 1,599,565 | **0.509585** | 影响 RQ5 库生态分析 |
| `baseModels.relation` | 2,251,923 | **0.717411** | 71.74% 的模型**完全没有 baseModel 记录** |
| `baseModels.models.list.element.id` | 2,251,923 | **0.717411** | 同上 |

> **重要推论**：`baseModels.relation` 与 `models[].id` 的 null 数完全相同，
> 说明 `baseModels` 要么整体为空（71.74%），要么整体有值（28.26%，即 887,036 个模型）。
> 这直接约束了 RQ3 谱系图的规模：**全量可用边只来自这 28.26% 的模型**。

### 3.2 其它字段（未进入契约，供后续按需启用）

| 字段 | 缺失率 | 备注 |
|---|---|---|
| `config` | 0.000001（3 行） | 几乎无缺失 |
| `siblings.list.element.rfilename` | 0.000226（708 行） | |
| `cardData` | 0.363305 | 旧 Demo 用的 base_model 来源 |
| `transformersInfo.auto_model` | 0.688199 | |
| `safetensors.total` | 0.712734 | 参数量，可用于模型规模分析 |
| `transformersInfo.pipeline_tag` | 0.793566 | |
| `gguf` | 0.933844 | |
| `trendingScore` | 0.000000 | |

> ⚠️ **统计口径注意**：`inferenceProviderMapping.*` 系列 leaf 的 null_count 除以行数会 **> 1**
> （如 1.027369），因为它是 list 列，null_count 统计的是**叶子值**而非行。
> 本文档因此**不把该系列列为行级缺失率**，避免误导。

---

## 4. 字段映射契约（Bronze → Silver）

| Bronze 字段 | Bronze 类型 | Silver 列 | Silver 类型 | 转换规则 |
|---|---|---|---|---|
| `id` | `large_string` | `model_id` | string | 直接映射 |
| `author` | `large_string` | `author` | string | 直接映射；若为空则从 `model_id` 的 `/` 前缀兜底 |
| `createdAt` | `timestamp[us]` | `created_at` | string | **→ ISO-8601 UTC 字符串** |
| `lastModified` | `timestamp[us]` | `last_modified` | string | **→ ISO-8601 UTC 字符串** |
| `downloads` | `int64` | `downloads` | long | 直接映射 |
| `downloadsAllTime` | `int64` | `downloadsAllTime` | long | **独立保留，不与 downloads 合并** |
| `likes` | `int64` | `likes` | long | 直接映射 |
| `pipeline_tag` | `large_string` | `pipeline_tag` | string | 直接映射 |
| `library_name` | `large_string` | `library_name` | string | 直接映射 |
| `tags` | `list<string>` | `tags` | array\<string\> | 直接映射 |
| *（源无此列）* | — | `gated` | string | **显式类型化 NULL** |
| `baseModels.models[].id` | `struct` | `base_models` | array\<string\> | `transform(baseModels.models, x -> x.id)` |
| `baseModels.relation` | `string` | `base_relation` | string | **原样保留**（finetune / adapter / quantized / merge …） |
| *（来自清单）* | — | `scraped_at` | string | `ingest_manifest.verified_at` |
| *（常量）* | — | `source` | string | `"cfahlgren1/hub-stats"` |
| *（来自清单）* | — | `snapshot_sha` | string | `ingest_manifest.commit` |

**Silver `models` 列顺序（`schema_adapter.SILVER_MODELS_COLUMNS`）：**

```
model_id, author, created_at, last_modified,
downloads, downloadsAllTime, likes,
pipeline_tag, library_name, tags,
gated, base_models, base_relation,
scraped_at, source, snapshot_sha
```

### 4.1 `downloads` vs `downloadsAllTime`（必须区分）

两者语义不同，**绝不合并**。抽样实测（50,218 行）：

```
sum(downloads)         =    37,080,330
sum(downloadsAllTime)  =   784,950,902      → 相差 21.17 倍
downloads == 0 的行数   =    21,835          → 占 43.5%
```

`downloads` 是"近 30 天窗口"口径，`downloadsAllTime` 是"历史累计"口径。
**RQ2 的注意力集中度必须明确声明用哪个口径**，否则结论不可比。

### 4.2 旧 Demo（jsonl）输入兼容

`local_etl.py` / `spark_etl.py` 同时支持旧 Demo 的 `models.jsonl`：

| 旧字段 | 映射 |
|---|---|
| `id` | `model_id` |
| `createdAt`（已是 ISO 字符串） | `created_at` 原样透传 |
| `cardData.base_model`（JSON 字符串，可能是单值或数组） | `base_models` → 统一成 `array<string>` |
| *（无）* | `base_relation` → NULL |
| `gated` | 直接读取 |

---

## 5. 时间字段契约

| 项 | 约定 |
|---|---|
| 目标格式 | ISO-8601 UTC：`yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`（常量 `schema_adapter.ISO_Z_FORMAT`） |
| 示例 | `2024-07-18T08:56:00.000Z` |
| Spark 会话时区 | **必须 `spark.sql.session.timeZone = UTC`** |

**为什么必须锁 UTC（这是实测踩到的坑）：**
不设 UTC 时，Spark 会话时区取本机 `Asia/Shanghai`。此时 jsonl 源的 `"...Z"` 字符串
经 `to_timestamp()` 解析成瞬时点，再用 `date_format()` 按 +08:00 渲染，
**跨月边界的记录月份会被平移**，导致 `task_monthly_stats` 行数从 248 变成 245，
与 pandas 的 UTC 口径不一致。

**同时必须覆盖 `TimestampNTZType`：**
hub-stats 的 parquet 里 `createdAt` 被 Spark 读成 `timestamp_ntz`，
它**不是** `TimestampType` 的实例。只判断 `TimestampType` 会漏判，
走到 `cast(StringType())` 分支，产出 `"2026-09-23 01:01:55"`（**无 `Z`、空格分隔**），
破坏 ISO-Z 契约。`iso_string_col()` 已改为同时匹配两者。

---

## 6. 路径与引擎隔离契约

```
<HF_DATA_DIR>/                     默认 <project>/data_hubstats
├── bronze/
│   ├── models.parquet             新正式原始数据（或 models.jsonl 旧数据）
│   └── ingest_manifest.json       来源 / commit / sha256 / 校验时间
├── silver/            ← HF_ENGINE=spark   引擎产物（三表）
├── gold/              ← HF_ENGINE=spark   引擎产物（七表 + models_enriched）
├── silver_local/      ← HF_ENGINE=local   引擎产物（三表）
└── gold_local/        ← HF_ENGINE=local   引擎产物（七表 + models_enriched）
```

**三重隔离，确保任何操作都不会覆盖旧结果：**

1. **数据源隔离**：新数据在 `<project>/data_hubstats/`，旧 Demo 在 `<project>/data/`，物理不同目录。
2. **引擎隔离**：Spark 写 `silver`/`gold`，pandas 写 `silver_local`/`gold_local`，互不覆盖
   （`local_etl._write_parquet` 还会先清掉目标目录里其它引擎遗留的 `part-*.parquet`，
   避免 `pd.read_parquet(目录)` 把两份数据一起读进来导致行数翻倍）。
3. **运行隔离**：抽样实验再套一层 `<root>/sample_run/`，回归实验套 `<root>/demo_regress/`。

---

## 7. 抽样契约

| 项 | 值 |
|---|---|
| 方法 | 确定性哈希：`crc32(str(model_id)) % 63 == 0` |
| 参数 | `schema_adapter.SAMPLE_MODULUS = 63` |
| 全量行数 | 3,138,959 |
| 抽样行数 | **50,218** |
| 实际抽样率 | **0.015998**（≈1/62.5） |
| 耗时 | 6.1 秒 |
| 产物 | `data_hubstats/bronze_sample/models.parquet` + `sample_manifest.json` |

**为什么用哈希而不是取前 N 行**：文件行组 0 是 trending 榜单（有偏），
哈希抽样与行序无关，可复现、可扩样、且统计上无偏。

**⚠️ 抽样的适用边界（本阶段实测发现）：**

抽样后 `children_count` 求和仅 **267**，而总边数为 **14,674** —— 即只有约 1.8% 的边，
其"父模型"恰好也落在样本内（`267/14,674 ≈ 1.8% ≈ 抽样率`）。

**结论：1.6% 的样本不足以支撑 RQ3 的家族谱系/连通分量分析**（父子链会被大量截断）。
RQ3 必须走全量，或改用"样本 + 父 id 定向回查"的策略。

---

## 8. Silver / Gold 表契约

### 8.1 Silver 三表

| 表 | 列 | 语义 |
|---|---|---|
| `models` | 见 §4（16 列） | 模型主表，`model_id` 唯一（已去重） |
| `model_tags` | `model_id`, `tag` | `tags` 展开长表，剔除 null |
| `model_base_edges` | `src`, `dst`, `relation` | 家族边：`src` = base model，`dst` = 派生模型；`relation` 为空时填 `"base_model"` |

### 8.2 Gold `models_enriched` 派生字段（§10 契约）

在 Silver `models` 基础上追加：

```
created_ts(timestamp)      modified_ts(timestamp)
model_age_days(int)        update_interval_days(int)
downloads_log(double)      likes_log(double)
tag_count(int)             year(int)
month_num(int)             ← 月份序号 1~12（注意列名是 month_num，不是 month）
created_month(string)      ← "yyyy-MM"
log_downloads(double)      ← downloads_log 的别名
children_count(long)       family_size(long)
```

> **列名契约警告**：整型月份列必须叫 **`month_num`**，字符串月份列叫 **`created_month`**。
> 旧 Demo 的 `models_enriched` 用的就是 `month_num`，pandas 路径也一直用 `month_num`。
> 若把整型列命名为 `month`，会与 `yyyy-MM` 字符串别名撞名，
> 导致 `groupBy("month")` 按名字解析到整型列（详见文档三 §6.1）。

### 8.3 Gold 七表

| 表 | 列 | 行数语义 |
|---|---|---|
| `task_monthly_stats` | `month`(yyyy-MM), `task`, `new_models` | 每月 × 任务的新增模型数；`pipeline_tag` 为空填 `"unknown"` |
| `library_monthly_stats` | `month`(yyyy-MM), `library`, `new_models` | 每月 × 库；`library_name` 为空填 `"unknown"` |
| `tag_monthly_stats` | `month`(yyyy-MM), `tag`, `new_models` | 每月 × 标签（`tags` 展开） |
| `download_distribution` | `model_id`, `downloads`, `downloadsAllTime`, `downloads_log`, `rank`, `cum_model_pct`, `cum_download_pct` | 按 `downloads` 升序的全局排名与累计占比，用于洛伦兹曲线/基尼系数 |
| `model_family_stats` | `family_size`, `num_models` | 家族规模分布 |
| `model_lifecycle` | `model_id`, `created_at`, `last_modified`, `model_age_days`, `update_interval_days` | 生命周期 |
| `model_adoption_features` | `model_id`, `model_age_days`, `likes`, `likes_log`, `pipeline_tag`, `library`, `tag_count`, `children_count`, `family_size`, `update_interval_days`, `label` | RQ4 特征表；`label` = `downloads_log` |

**空值填充约定（两引擎必须一致）**：`task` 与 `library` 的空值统一填字符串 `"unknown"`，
不允许一边填 `"unknown"`、一边保留 SQL NULL（否则同一张表在两引擎下标签不同）。

---

## 9. 向后兼容验证

用旧 Demo 的 `data/bronze/models.jsonl`（1000 行）在隔离目录跑双引擎，结果与 Phase 0 审计记录的旧结果完全一致：

| 指标 | 旧 Demo（Phase 0 记录） | Phase 1A 复现 | 一致 |
|---|---|---|---|
| 模型数 | 1000 | 1000 | ✅ |
| base model 边数 | 71 | 71 | ✅ |
| Gini(downloads) | 0.928 | **0.927954** | ✅ |
| `task_monthly_stats` 行数 | 248 | 248 | ✅ |

→ **旧数据通路未被破坏，旧结果未被覆盖**（`data/` 目录文件时间戳全部保持 2026-10-10 及更早）。
