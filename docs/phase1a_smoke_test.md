# Phase 1A 交付文档（三）：小规模真实数据端到端冒烟测试

> 工程：`hf_eco_analysis`　｜　阶段：Phase 1A　｜　日期：2026-10-11
> 本文记录**实际执行过**的每一步及其真实输出。所有数字均来自真实数据与真实日志，无任何虚构。

---

## 1. 执行清单与耗时

| # | 步骤 | 命令 | 结果 | 耗时 |
|---|---|---|---|---|
| 1 | 下载正式数据 | `python download_hubstats.py` | ✅ 1,596,738,497 B，SHA-256 匹配 | 未单独计时（见 §2） |
| 2 | 确定性哈希抽样 | `python sample_bronze.py` | ✅ 50,218 / 3,138,959 行 | **6.1 s** |
| 3 | Gini 公式等价性验证 | `python test_gini.py` | ✅ 最大绝对差 5.551e-16 | **1.97 s** |
| 4 | Spark local 最小测试 | `python spark_smoke.py` | ✅ `roundtrip_ok=true` | **124.25 s**（会话 39.0 / 写 8.2 / 读 1.62） |
| 5 | pandas 引擎 ETL | `HF_ENGINE=local python local_etl.py` | ✅ 50218 模型 / 327036 标签 / 14674 边 | **34.52 s** |
| 6 | Spark 引擎 ETL | `HF_ENGINE=spark python spark_etl.py` | ✅ 同上，Gold 50218 行 | **193 s**（含 39 s 冷启动） |
| 7 | 双引擎一致性校验 | `python verify_phase1a.py --root .../sample_run` | ✅ 13/13 项全一致 | **5.49 s** |
| 8 | 旧 Demo jsonl 回归 | 两引擎 × `data/bronze/models.jsonl` | ✅ 13/13 项全一致，复现旧结果 | 34 s / ~120 s |

环境：`local[4]`、driver 4g、shuffle 16、`fs.file.impl=BareLocalFileSystem`、`session.timeZone=UTC`。

---

## 2. 下载与校验结果

| 项 | 值 |
|---|---|
| 字节数 | 1,596,738,497（与预期完全一致） |
| SHA-256 | `d86485a02a7aace1b8c4eda4a849dd153a702d5351ab5a57f3c2320176e251ef` |
| 校验 | ✅ 与可信元数据**完整匹配**（全文件哈希，非前缀） |
| Parquet 可读性 | ✅ `pyarrow.parquet.ParquetFile` 正常打开 |
| 实际行数 | **3,138,959** |
| Schema | 20 个顶层字段 / 69 个 leaf 列 / 63 个 row group |
| 采集记录 | `data_hubstats/bronze/ingest_manifest.json`（含来源、commit、hash、校验时间） |

> 下载脚本支持**断点续传**（HTTP Range + 已下载字节校验），
> 支持**全量 SHA-256 复核**，并把来源/commit/文件哈希/采集时间写入清单。

---

## 3. 抽样结果

```
方法     : crc32(str(model_id)) % 63 == 0
全量行数 : 3,138,959
抽样行数 : 50,218
实际抽样率: 0.015998
耗时     : 6.1 s
产物     : data_hubstats/bronze_sample/models.parquet
           data_hubstats/bronze_sample/sample_manifest.json
```

抽样实验在**独立根目录** `data_hubstats/sample_run/` 下运行，
与将来的全量结果（`data_hubstats/silver`、`data_hubstats/gold`）完全隔离。

---

## 4. 双引擎 ETL 结果（每张表行数）

样本规模：50,218 行。**两个引擎产出的行数逐表完全一致。**

| 层 | 表 | Spark | pandas | 一致 |
|---|---|---|---|---|
| Silver | `models` | 50,218 | 50,218 | ✅ |
| Silver | `model_tags` | 327,036 | 327,036 | ✅ |
| Silver | `model_base_edges` | 14,674 | 14,674 | ✅ |
| Gold | `models_enriched` | 50,218 | 50,218 | ✅ |
| Gold | `task_monthly_stats` | 1,343 | 1,343 | ✅ |
| Gold | `library_monthly_stats` | 1,277 | 1,277 | ✅ |
| Gold | `tag_monthly_stats` | 56,328 | 56,328 | ✅ |
| Gold | `download_distribution` | 50,218 | 50,218 | ✅ |
| Gold | `model_family_stats` | 9 | 9 | ✅ |
| Gold | `model_lifecycle` | 50,218 | 50,218 | ✅ |
| Gold | `model_adoption_features` | 50,218 | 50,218 | ✅ |

**关键字段缺失率（Silver `models`，两引擎相同）：**

| 字段 | 缺失率 | 全量对照（§文档二） |
|---|---|---|
| `author` | 0.000000 | 0.000000 |
| `created_at` | 0.000000 | 0.000000 |
| `last_modified` | 0.000000 | 0.000000 |
| `downloads` | 0.000000 | 0.000000 |
| `downloadsAllTime` | 0.000000 | 0.000000 |
| `likes` | 0.000000 | 0.000000 |
| `tags` | 0.000000 | 0.000000 |
| `pipeline_tag` | 0.656737 | 0.655549 |
| `library_name` | 0.511012 | 0.509585 |
| `base_models` | 0.717711 | 0.717411 |
| **`gated`** | **1.000000** | 源无此列 → 显式 NULL ✅ |

→ 样本缺失率与全量缺失率**高度吻合**（偏差 < 0.2 个百分点），说明哈希抽样无偏。

**核心统计量（50,218 行样本，两引擎完全相同）：**

```
sum(downloads)          =    37,080,330
sum(downloadsAllTime)   =   784,950,902
downloads == 0 的行数    =    21,835            (43.5%)
created_at 范围         = 2022-03-02 23:29:04 UTC  →  2026-10-10 13:08:04 UTC
created_at 解析失败      =    0
Gini(downloads)         =    0.994445
Gini(downloadsAllTime)  =    0.997149
Top-1% 下载份额          =    0.966011          (96.6%)
唯一标签数               =    26,599
边数 / 不同父模型数       =    14,674 / 4,910
边 relation 分布         =    finetune 5,605 | adapter 5,296 | quantized 3,048 | merge 725
模型 base_relation 分布  =    (空) 36,042 | finetune 5,605 | adapter 5,296 | quantized 3,048 | merge 227
children_count 求和      =    267                ← 抽样导致父子链大量截断，见 §7
```

---

## 5. 双引擎一致性校验（13/13 通过）

`verify_phase1a.py` 输出（`data_hubstats/sample_run/phase1a_verify_report.json`）：

```
n_models                spark 50218     local 50218     match true
n_tag_rows              spark 327036    local 327036    match true
n_edges                 spark 14674     local 14674     match true
downloads_sum           spark 37080330  local 37080330  match true
downloadsAllTime_sum    spark 784950902 local 784950902 match true
gini_downloads          spark 0.994445  local 0.994445  abs_diff 0.0
gold.task_monthly_stats spark 1343      local 1343      match true
gold.library_monthly_stats spark 1277   local 1277      match true
gold.tag_monthly_stats  spark 56328     local 56328     match true
gold.download_distribution spark 50218  local 50218     match true
gold.model_family_stats spark 9         local 9         match true
gold.model_lifecycle    spark 50218     local 50218     match true
gold.model_adoption_features spark 50218 local 50218    match true

[OK] Phase 1A 验收检查通过
```

**硬性断言（全部通过）：**

| 断言 | 结果 |
|---|---|
| `model_id` 无重复（`dup_model_id`） | ✅ 0 |
| `gated` 保持全 NULL | ✅ true |
| `created_at` 全部可解析为 timestamp | ✅ 0 条失败 |
| `created_at` 全部符合 ISO-8601 UTC（`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`） | ✅ 两引擎均 true |
| `task_monthly_stats.month` 是 `yyyy-MM` 字符串 | ✅ true |
| `models_enriched` 含 `month_num` 列（与旧 Demo 契约一致） | ✅ true |
| `family_size >= 1` | ✅ true |

**旧 Demo jsonl 回归（隔离目录 `data_hubstats/demo_regress/`，1000 行）：13/13 项同样全一致**，
并精确复现了旧结果（模型 1000 / 边 71 / `task_monthly_stats` 248 / Gini 0.927954）。

---

## 6. 本阶段发现并修复的缺陷

> **诚实说明**：工程无版本控制（无 `.git`），无法用 diff 证明缺陷来源。
> 但通过比对**旧 Demo 仍在盘上的 Gold 产物**（`data/gold/models_enriched` 的月份列名为 `month_num`、
> `data/gold/task_monthly_stats.month` 为 `2022-03` 形态），可以确认下列缺陷 1、2 是
> **本次 Phase 1A 改写 `spark_etl.py` 时引入的回归**，缺陷 3 是原有实现一直存在的时区口径问题。

### 6.1 缺陷 1：`groupBy("month")` 被解析到整型月份列（**已修复**）

**现象**：`task_monthly_stats` 行数在 Spark 下 492、pandas 下 1343（parquet 路径）；
jsonl 回归下 Spark 245、pandas 248。

**根因**：改写时把派生整型月份列命名为 `month`（旧契约是 `month_num`），
而 `task_monthly_stats` 的代码写的是 `groupBy("month", ...)`。
Spark 的 `groupBy` 用**字符串**参数时按列名解析，于是命中了 `base_gold` 里的**整型** `month`（1~12），
而不是 `yyyy-MM` 字符串表达式 → 分组粒度从"月"退化成"月份序号"。

**证据**：492 ≈ 12 个月份序号 × ~41 个任务；1343 ≈ 56 个 `yyyy-MM` × ~24 个任务。

**修复**（`spark_etl.py`）：

```python
# 整型列改回 month_num（与旧 Demo / pandas 路径一致）
.withColumn("month_num", F.month("created_ts"))

# 分组必须传 Column 对象，不能传字符串 "month"
month_col = F.date_format("created_ts", "yyyy-MM").alias("month")
base_gold.groupBy(month_col, F.coalesce(F.col("pipeline_tag"), F.lit("unknown")).alias("task"))
```

**验证**：修复后 parquet 路径 1343 = 1343 ✅，jsonl 路径 248 = 248 ✅（与旧 Demo 完全一致）。

### 6.2 缺陷 2：`created_at` 缺少 `Z`、空格分隔，破坏 ISO-8601 契约（**已修复**）

**现象**：Spark 产出的 `created_at` 形如 `2026-09-23 01:01:55`，
而 pandas 与旧 Demo 产出 `2024-07-18T08:56:00.000Z`。

**根因**：hub-stats 的 parquet 里 `createdAt` 被 Spark 读成 **`timestamp_ntz`**，
它不是 `TimestampType` 的实例，于是 `iso_string_col()` 的
`isinstance(df.schema[name].dataType, TimestampType)` 判断为假，
走了 `F.col(name).cast(StringType())` 分支（默认 `toString` 格式）。

**修复**（`spark_etl.py`）：

```python
if isinstance(df.schema[name].dataType, (TimestampType, TimestampNTZType)):
    return F.date_format(F.col(name).cast("timestamp"), SA.ISO_Z_FORMAT).alias(name)
```

**验证**：修复后两引擎 `created_at_iso_z_ok = true` ✅。

### 6.3 缺陷 3：Spark 会话时区非 UTC，跨月记录被平移（**已修复**）

**现象**：jsonl 路径下 `task_monthly_stats` Spark 245 / pandas 248。

**根因**：会话时区默认取本机 `Asia/Shanghai`。
`to_timestamp("2026-09-30T20:00:00.000Z")` 得到瞬时点后，
`date_format()` 按 +08:00 渲染成 `2026-10-01` → **月份被平移**，与 pandas 的 UTC 口径不一致。

**修复**（`spark_etl.py` / `spark_smoke.py`）：

```python
.config("spark.sql.session.timeZone", "UTC")
```

**验证**：修复后 jsonl 路径 248 = 248 ✅。

### 6.4 接入坑：`spark.jars` 传递文件系统 jar 会造成死锁（已绕开）

`spark.jars` 的路径解析发生在 `SparkSubmit.prepareSubmitEnvironment`，
而解析 glob 本身需要先创建 `FileSystem` → 反过来去加载 `fs.file.impl` 指定的类 →
此时该类还没进类路径 → `ClassNotFoundException`。
**唯一可行做法是放进 `<pyspark>/jars/`**（由 `setup_spark_fs.py` 完成）。详见文档一 §5.5。

### 6.5 为防复发而新增的自动校验

`verify_phase1a.py` 新增三类校验，正是它们发现了上述缺陷：

1. Gold 七表行数逐表比对（原来只比对 Silver）
2. `task_monthly_stats.month` 必须是 `yyyy-MM` 字符串
3. `created_at` 必须匹配 ISO-8601 UTC 正则
4. `models_enriched` 必须含 `month_num` 列

---

## 7. Gini 公式等价性验证（按要求先做数学验证，不靠假设）

`test_gini.py` 用**已知数组**直接验证两式等价，而不是假设"零值会导致不一致"：

| 测试数组 | n | 秩式 | 累计式 | 绝对差 |
|---|---|---|---|---|
| `[0,10]` | 2 | 0.5000000000 | 0.5000000000 | 0.00e+00 |
| `[0,0,1]` | 3 | 0.6666666667 | 0.6666666667 | 1.11e-16 |
| `[1,2,3]` | 3 | 0.2222222222 | 0.2222222222 | 0.00e+00 |
| `[0,0,0,5,5]` | 5 | 0.6000000000 | 0.6000000000 | 1.11e-16 |
| `[1]*10`（全相等） | 10 | 0.0000000000 | 0.0000000000 | 0.00e+00 |
| `[0]*10`（全零） | 10 | 0.0000000000 | 0.0000000000 | 0.00e+00 |
| `[0]*90+[1]+[1000]`（重尾） | 92 | 0.9891087174 | 0.9891087174 | 0.00e+00 |
| `[0]*99+[100]`（极端洛伦兹） | 100 | 0.9900000000 | 0.9900000000 | 0.00e+00 |
| 随机均匀 | 100 | 0.3221203180 | 0.3221203180 | 4.44e-16 |
| 随机长尾 | 200 | 0.6660485857 | 0.6660485857 | 5.55e-16 |

```
最大绝对差 = 5.551e-16
[OK] 两式在全部测试数组上等价（差异在浮点误差内）
```

**结论**：秩式 `2S/(n·total) − 1`（`S = Σ(rank−0.5)·x`）与累计式 `(n+1−2Σcum/total)/n`
在数学上等价；**零值、全零、全相等都不会导致两式不一致**。
唯一需要防范的是"`downloads` 全为 0 时除零"，已在实现中对 `total == 0` 单独返回 0.0。

---

## 8. 变更文件清单

### 新增（8 个脚本 + 1 个 jar）

```
download_hubstats.py        断点续传下载 + 全量 SHA-256 校验 + 写采集清单
schema_adapter.py           统一 Schema 契约与路径解析层（不 import pyspark/pandas）
sample_bronze.py            确定性哈希抽样
spark_smoke.py              Spark local 最小测试
setup_spark_fs.py           把纯 Java 文件系统 jar 装进 <pyspark>/jars/
test_gini.py                Gini 两式等价性验证
verify_phase1a.py           Phase 1A 端到端验收校验
jars/hadoop-bare-naked-local-fs-0.1.0.jar
jars/hadoop-bare-naked-local-fs-0.1.0.jar.sha1 / .asc / -sources.jar
```

### 修改（3 个）

```
spark_etl.py    接入 schema_adapter；parquet/jsonl 双源 Bronze 适配；
                新增 base_relation / source / snapshot_sha 列；gated 显式类型化 NULL；
                新增 FS / 时区 / master / 内存配置；修复缺陷 1、2、3
local_etl.py    接入 schema_adapter；新增 parquet 输入；产物按引擎隔离；
                保留 GOLD / DATA_DIR / build_gold / load_bronze 导出（local_analysis.py 依赖）
verify_phase1a.py  新增 Gold 七表行数 / 月份格式 / ISO-Z 格式校验
```

### 严格未改动

`analysis_evo.py`、`analysis_graph.py`、`analysis_ml.py`、`analysis_rq5.py`、`benchmark.py`、
`collect_hf.py`、`enrich_lastmodified.py`、`local_analysis.py`、`make_figures.py`、
`make_bench_figures.py`、`dashboard.html`、`report.md`、`README.md`、`requirements.txt`，
以及 **`data/` 下旧 Demo 的全部原始数据与结果**（已用文件时间戳验证：无任何 parquet 在本次会话时间窗内被修改）。

### 新增数据目录（全部与旧结果隔离）

```
data_hubstats/bronze/          正式原始数据 + ingest_manifest.json
data_hubstats/bronze_sample/   50,218 行抽样 + sample_manifest.json
data_hubstats/sample_run/      抽样实验（silver / gold / silver_local / gold_local）
data_hubstats/demo_regress/    旧 Demo jsonl 回归（隔离）
data_hubstats/_smoke/          所有日志与报告
_hadoop_home/                  排查证据目录（仅 0 字节占位文件 + NOTE.txt）
```

---

## 9. 全量运行（3,138,959 行）风险与建议

| 风险 | 具体说明 | 建议 |
|---|---|---|
| **① 全局排序窗口塌成单分区** | `download_distribution` 使用 `Window.orderBy("downloads","model_id")` 且**无 `partitionBy`**，Spark 会告警 `No Partition Defined for Window operation! Moving all data to a single partition`。这是基尼系数/洛伦兹曲线的**语义要求**（必须全局排名），不能靠加分区消除，但 314 万行会全部涌入 1 个分区。 | 用 `local[16]` + driver 8g；若超时，改为"分桶排序 + 分段累计"重写 |
| **② 抽样不足以支撑 RQ3** | 1.6% 样本下 `children_count` 求和仅 267，而边数 14,674 —— 仅约 1.8% 的边其父模型也在样本内。谱系链被大量截断。 | RQ3 走全量，或"样本 + 父 id 定向回查" |
| **③ shuffle 分区数偏小** | 当前默认 16，对 314 万行偏小 | 全量跑时按 16 / 32 / 64 扫描调优 |
| **④ pandas 路径内存** | `local_etl.py` 整表进内存；314 万行 + explode 出约 2000 万标签行 | 全量优先走 Spark，pandas 仅作抽样对拍 |
| **⑤ driver 内存** | 默认 4g | 全量上调至 8g（必要时 12g） |
| **⑥ SparkSession 冷启动 ~39 s** | 本机实测 | 把全量 ETL + 后续 RQ 分析合并到一次会话内 |
| **⑦ 缺失率影响结论** | `pipeline_tag` 缺失 65.6%、`library_name` 缺失 51.0%、`base_models` 缺失 71.7%（**全量口径**） | 所有按任务/库分组的结论必须同时报告缺失占比，不能把 `"unknown"` 当作真实任务 |
| **⑧ 下载/口径混用** | `downloads`（近 30 天）与 `downloadsAllTime`（累计）相差 21.17 倍 | RQ2 结论必须显式声明所用口径 |
| **⑨ 不允许把 Trending 样本当总体** | 文件行组 0 是 trending 榜 | 已全流程禁止 `head(N)`；抽样用哈希 |

---

## 10. 停止点声明

按用户要求：

> **Phase 1A 完成后立即停止，等待验收。不自动执行 314 万条全量任务。**

✅ 本阶段已停止。
- **未执行**任何全量（3,138,959 行）ETL。
- **未执行**GraphFrames 图计算。
- **未执行**MLlib 训练。
- **未执行** Benchmark。
- **未覆盖**任何旧数据或旧结果。

等待验收后再进入 Phase 1B。
