# Hugging Face 开源模型生态的演化、注意力集中与模型谱系分析

> 大学《大数据导论》课程项目 · 对应 `proposal.pdf` 的 **Proposal 2**
> 实现引擎：**PySpark 4.2.0 本地模式**（Windows 11）· 数据源：Hugging Face Hub 公开数据集快照

本 README 是**唯一的复现入口**。所有实验数字的权威来源是
`data_hubstats/gold/analysis/results_manifest.json`（每条指标都带出处），
报告、Dashboard、图注、LaTeX/PPT 素材全部引用它，不手抄、不估算。

---

## 1. 研究问题（RQ）

| 编号 | 问题 | 主要产物 |
| --- | --- | --- |
| RQ1 | 模型生态如何随时间演化（月度新增、任务/库/标签趋势） | `monthly_new_models.csv`、`pipeline_trend.csv`、`summary_rq1.json` |
| RQ2 | 下载注意力有多集中？长尾形态如何？ | `concentration_stats.json`、`lorenz_curve.csv`、`ccdf.csv`、`summary_rq2.json` |
| RQ3 | 模型谱系网络结构（底座→衍生） | `graph_stats.json`、`graphframes_result.json`、`node_metrics/` |
| RQ4 | 模型属性与下载热度的**统计关联**（非前瞻预测） | `ml_summary.json`、`ml_model_comparison/`、`ml_feature_importance_*` |
| RQ5 | 不同任务生态的结构差异 | `rq5_task_ecosystem.csv`、`rq5_summary.json` |

---

## 2. 数据源（固定快照，可校验）

| 项 | 值 |
| --- | --- |
| 数据集 | `cfahlgren1/hub-stats`（Hugging Face Hub 公开数据集） |
| 文件 | `models.parquet` |
| commit | `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70` |
| SHA-256 | `d86485a02a7aace1b8c4eda4a849dd153a702d5351ab5a57f3c2320176e251ef` |
| 大小 | 1,596,738,497 bytes（约 1.49 GiB） |
| 物理行数 | 3,138,959（其中 5 个 `id` 重复 → 去重后 3,138,954 个模型） |
| 下载镜像 | `hf-mirror.com`（本机直连 huggingface.co 不通） |
| 落地位置 | `data_hubstats/bronze/models.parquet` |
| 下载凭据 | `data_hubstats/bronze/ingest_manifest.json`（含 sha256 校验结果） |

**已校验**：`ingest_manifest.json` 中 `sha256_match: true`。

> ⚠️ 复现时**不需要重新下载 314 万条模型明细**。只要 `data_hubstats/bronze/models.parquet`
> 存在且 SHA-256 与上表一致，即可从第 4 节第 ② 步直接开始。

---

## 3. 运行环境

### 3.1 本机硬件与系统

| 项 | 值 |
| --- | --- |
| 操作系统 | Windows 11（Build 26200） |
| CPU | Intel Core Ultra 7 255HX（20 逻辑核） |
| 物理内存 | 31.43 GiB（**注意：物理内存 ≠ 可用内存**，运行前务必查可用量） |
| Java | OpenJDK 17（`E:/Software/jdk-17`） |
| WSL | **无**（不依赖 WSL） |
| winutils.exe | **无**（用纯 Java 文件系统替代，见 3.4） |

### 3.2 conda 环境

```
环境名   hf_eco
路径     E:/Users/Admin/anaconda3/envs/hf_eco
Python   3.11.17
解释器   E:/Users/Admin/anaconda3/envs/hf_eco/python.exe
```

关键版本（`requirements.txt` 为准）：

```
pyspark            4.2.0     （内置 Hadoop 3.5.0、Scala 2.13、spark-graphx_2.13-4.2.0）
graphframes-py     0.12.3
pandas             3.0.6
pyarrow            26.0.0
networkx           3.6.1
matplotlib         3.11.2
psutil             7.2.2
```

> 依赖装在独立 conda 环境内，**不污染 base**。

### 3.3 额外 JVM 构件（已随工程放在 `jars/`）

| jar | 用途 |
| --- | --- |
| `hadoop-bare-naked-local-fs-0.1.0.jar` | 纯 Java 本地文件系统，绕开 winutils.exe |
| `graphframes-spark4_2.13-0.12.3.jar` | GraphFrames 主构件 |
| `graphframes-graphx-spark4_2.13-0.12.3.jar` | GraphFrames 的 GraphX 后端 |

这三个 jar 必须位于 **`<pyspark>/jars/`** 目录内，由 `setup_spark_fs.py` 幂等安装：

```bash
E:/Users/Admin/anaconda3/envs/hf_eco/python.exe setup_spark_fs.py
```

> ❌ **不能**用 `spark.jars` / `spark.driver.extraClassPath` 传 `hadoop-bare-naked-local-fs`：
> 路径解析发生在 `SparkSubmit.prepareSubmitEnvironment` 阶段，解析 glob 需要先创建
> FileSystem，于是反过来去加载 `fs.file.impl` 指定的类，此时该类还没进类路径，
> 抛 `ClassNotFoundException` 形成死锁。只能让它在 JVM 启动最初就在类路径上。

### 3.4 为什么 Windows 上不需要 winutils（重要，勿重复踩坑）

反编译 PySpark 内置的 `hadoop-client-api-3.5.0.jar` 证实：
`RawLocalFileSystem.setPermission` 在 Windows 上只有两条分支 ——
`NativeIO$POSIX.chmod`（需 `hadoop.dll`）或 `Shell.execCommand`（需 `winutils.exe`）；
而 `mkOneDirWithMode` 在权限为 null 时**强制**填入 `FsPermission.getDirDefault()`，
`core-default.xml` 中**没有任何开关**能关闭这次调用 → **无法用配置绕过**。

**解法**：使用 `com.globalmentor:hadoop-bare-naked-local-fs:0.1.0`
（Apache-2.0 / Maven Central / 已做源码与字节码审计，无网络访问、无外部进程）。
其 `setPermission` 走 `Files.getFileAttributeView(PosixFileAttributeView)`，
Windows 上该 view 返回 `null` → 空操作，完全不碰原生库与外部进程。

配置项：`spark.hadoop.fs.file.impl=com.globalmentor.apache.hadoop.fs.BareLocalFileSystem`

### 3.5 必须锁定 UTC 时区

`spark.sql.session.timeZone` **必须**设为 `UTC`，否则源数据里 `"...Z"` 的时间会按本机
Asia/Shanghai 渲染，23:xx 的 UTC 记录被平移到次月，与 pandas 路径口径不一致。
该配置已统一写在 `spark_session.py`，所有 Spark 脚本共用。

---

## 4. 完整执行顺序（复现步骤）

所有命令在项目根目录执行；`PY` 指代
`E:/Users/Admin/anaconda3/envs/hf_eco/python.exe`。

```bash
# ⓪ 一次性：把额外 jar 装进 <pyspark>/jars/（幂等，可重复执行）
$PY setup_spark_fs.py

# ① 数据落地（若 data_hubstats/bronze/models.parquet 已存在且 sha256 一致，跳过）
#    下载脚本会校验 sha256 并写 ingest_manifest.json

# ② Bronze -> Silver -> Gold 全量 ETL（3,138,959 行）
HF_ENGINE=spark HF_SPARK_MASTER="local[6]" HF_SPARK_DRIVER_MEMORY="4g" \
  HF_SHUFFLE_PARTITIONS=64 $PY run_full_etl.py

# ③ 全量数据质量与守恒检查 -> data_quality_report.json
HF_ENGINE=spark $PY verify_etl.py

# ④ Phase 2：RQ1/RQ2 -> RQ3 -> RQ4 -> RQ5（顺序固定，前者的产物是后者的输入）
$PY run_phase2.py
#    失败重跑单个步骤：$PY run_phase2.py --only ml,rq5

# ⑤ RQ3 的 GraphFrames 迭代算法（长任务，带超时看门狗，可断点续跑）
HF_SPARK_MASTER="local[6]" HF_SPARK_DRIVER_MEMORY="4g" $PY analysis_graph_gf.py

# ⑥ Phase 3：Spark 扩展性实验（真实数据规模扫描 + 并行度扫描 + 合成压力测试）
$PY run_benchmark.py                 # 全部三组
$PY run_benchmark.py --phases p1     # 只跑真实数据规模扫描

# ⑦ Phase 5：数据契约与算法正确性测试（需要真实数据）
$PY test_algorithms.py               # 纯 Python 算法等价性（不需要 Spark）
$PY test_spark_contracts.py          # Spark 侧契约（A–H 组，需真实数据）

# ⑧ Phase 4：12 张正式图 + Dashboard + 图目录
$PY make_figures.py                  # -> figures_final/ 与 dashboard_final.html
$PY make_bench_figures.py            # 仅重绘 Figure 10–12（委托给 make_figures）

# ⑨ 结果清单 + 报告 + 一致性检查（顺序不可颠倒）
$PY build_manifest.py                # -> results_manifest.json（唯一结果基准）
$PY write_reports.py                 # -> 7 份报告（全部从清单读取）
$PY check_consistency.py             # 校验各交付物数字一致、无旧 Demo 残留，含 Task E 扩展（GraphFrames 状态、图表一致性、生成器防回退、数据一致性、避免硬编码 + 人工复核清单）
```

### 4.1 环境变量总表

| 变量 | 默认值 | 作用 |
| --- | --- | --- |
| `HF_DATA_DIR` | `<project>/data_hubstats` | 数据根目录（与旧 Demo 的 `data/` 隔离） |
| `HF_ENGINE` | `spark` | `spark` → `silver`/`gold`；`local` → `silver_local`/`gold_local` |
| `HF_BRONZE_PATH` | 自动解析 | 显式指定 Bronze 输入（文件或目录） |
| `HF_BARE_FS` | `1` | 是否启用纯 Java 本地文件系统（`0/false/no/off` 关闭） |
| `HF_SPARK_MASTER` | `local[6]` | Spark master |
| `HF_SPARK_DRIVER_MEMORY` | `3g` | driver 内存（Phase 2/3 编排器显式用 `4g`） |
| `HF_SHUFFLE_PARTITIONS` | `64` | shuffle 分区数 |
| `HF_LOCAL_SAMPLE_PCT` | `100` | **local 引擎**选样百分比（`crc32(id)%100 < pct`） |
| `HF_REACH_HOPS` | `3` | RQ3 中 ≤K 跳可达后代的 K |
| `HF_GF_BUDGET_SEC` | `2400` | GraphFrames 看门狗预算（秒） |
| `HF_VIZ_MAX_NODES` | `1500` | 谱系可视化子图最多绘制的节点数 |

### 4.2 引擎隔离与结果隔离

* `HF_ENGINE=spark` → `data_hubstats/silver`、`data_hubstats/gold`
* `HF_ENGINE=local` → `data_hubstats/silver_local`、`data_hubstats/gold_local`
* 抽样运行 → `data_hubstats/sample_run/`；jsonl 回归 → `data_hubstats/demo_regress/`

两套引擎产物落在不同目录，**互不覆盖**；旧 Demo 的 `data/`、`figures/`、
`dashboard.html`、`report.md` 全程只读、从未改动。

---

## 5. 目录结构

```
hf_eco_analysis/
├── data/                    旧 Demo 数据（只读保留，不被正式流程引用）
├── data_hubstats/
│   ├── bronze/              models.parquet + ingest_manifest.json
│   ├── silver/              models / model_tags / model_base_edges
│   ├── gold/                七表 + lorenz_points + concentration_stats.json
│   │   └── analysis/        RQ1–RQ5、GraphFrames、Benchmark、结果清单
│   ├── sample_run/          抽样运行隔离目录
│   └── demo_regress/        jsonl 回归隔离目录
├── figures/                 旧 Demo 图（只读保留）
├── figures_final/           Fig01–Fig12（PNG 300dpi + PDF + SVG）
├── figures_supplementary/   补充材料图（旧版 Figure 7，不属正式图，不进 figure_index）
├── dashboard_final.html     离线看板（打开时**不运行 Spark**）
├── dashboard_standalone.html 单文件看板（图片 base64 内嵌，可单独分享）
├── jars/                    额外 JVM 构件（见 3.3）
├── docs/                    各阶段文档与报告
├── report_final.md          最终报告
└── README.md                本文件
```

---

## 6. 脚本清单

| 脚本 | 职责 |
| --- | --- |
| `schema_adapter.py` | 列名/路径/快照常量的**唯一**定义处（不 import pyspark/pandas） |
| `spark_session.py` | **唯一**的 SparkSession 构建入口（UTC + 纯 Java FS + Python 版本锁定） |
| `setup_spark_fs.py` | 把额外 jar 幂等安装进 `<pyspark>/jars/` |
| `run_full_etl.py` | Bronze→Silver→Gold 全量 ETL，写运行报告与内存采样 |
| `verify_etl.py` | 全量质量检查：行数、守恒、缺失率、时间可解析、主键重复 |
| `analysis_evo.py` | RQ1 + RQ2 |
| `analysis_graph.py` | RQ3 的 Spark 原生图统计（出/入度、可达、外部父模型、自环） |
| `analysis_graph_gf.py` | RQ3 的 GraphFrames 迭代算法（可断点续跑 + 超时看门狗） |
| `analysis_ml.py` | RQ4：先划分后拟合的 MLlib 训练与独立测试集评估 |
| `analysis_rq5.py` | RQ5 任务生态对比（频数表精确分位数） |
| `benchmark.py` | Phase 3 单次实验（real / synth 两种模式） |
| `run_benchmark.py` | Phase 3 编排器（三组实验 + 合并 + 元信息） |
| `run_phase2.py` | Phase 2 编排器（RQ1→RQ3→RQ4→RQ5，失败即中止） |
| `stats_utils.py` | 集中度/分位数/选样的**单一口径**实现 |
| `test_algorithms.py` | 纯 Python 算法等价性测试 |
| `test_spark_contracts.py` | Spark 侧数据契约测试（A–H 组） |
| `make_figures.py` | 12 张正式图 + 补充材料图 + Dashboard + 图目录 |
| `make_bench_figures.py` | 仅重绘 Figure 10–12（委托 `make_figures`） |
| `check_figure_quality.py` | 图表**排版**检查：重渲染全部正式图并检测文字重叠/越界（不落盘、不改交付图） |
| `build_manifest.py` | 汇总所有结果为唯一清单，并**推导**最终状态 |
| `write_reports.py` | 从清单生成 7 份报告（含 LaTeX/PPT 素材） |
| `check_consistency.py` | 交付物数字一致性 + 旧 Demo 污染检查 + 图表/Dashboard/生成器一致性 + Task E 扩展 |
| `docs/final_refinement_report.md` | 本轮修复的问题清单、验证结果与遗留项（缺陷 vs 优化分类） |
| `docs/visual_quality_review.md` | 12 张正式图逐张视觉审查结论 |
| `docs/manual_review_checklist.md` | 机器无法判定的语义复核清单（由 `check_consistency.py` 生成） |
| `local_etl.py` / `local_analysis.py` | pandas/networkx 等价路径（**仅用于验证与可视化**） |

---

## 7. 关键口径（结论成立的前提）

### 7.1 两种下载口径，绝不混用

| 字段 | 含义 | 合计 |
| --- | --- | --- |
| `downloads` | **近 30 天**下载计数 | 2,788,919,780 |
| `downloadsAllTime` | **历史累计**下载计数 | 75,949,397,771 |

两者相差约 27 倍。任何集中度结论都必须声明所用口径。

### 7.2 集中度算法

Gini 用**频数加权秩公式**精确计算（`G = 2S/(n·total) − 1`），不是采样近似；
Top-k 用显式排名定义（按值降序取前 `max(1, int(n·p))` 个模型，并列值整体同权，结果可复现）。
`test_algorithms.py` 证明三种独立口径（秩式 / 频数加权 / Lorenz 梯形积分）互相等价，
最大偏差 ≤ 4.4e-16。

> 本分析**不宣称幂律**：只描述长尾形态（Lorenz 偏离、CCDF 双对数近似直线），
> 未做严格幂律拟合与模型比较。

### 7.3 选样（local 与 Spark 必须选中同一批行）

统一规则 `crc32(id) % 100 < pct`。Python 的 `zlib.crc32` 与 Spark 的 `pmod(F.crc32(id), 100)`
在真实 `model_id` 上选中**完全相同**的行集（`test_spark_contracts.py` H 组实测校验），
因此各规模档之间只差数据量、不差样本构成，且 10% ⊂ 25% ⊂ 50% ⊂ 100% 为嵌套子集。

### 7.4 边语义（RQ3）

`src` = base model（被继承的底座），`dst` = 衍生模型。**方向不可反**。

* `children_count` = 出度 = 一个底座被多少模型**直接**当 base
* `direct_family_size` = `children_count + 1`
* `descendant_count_khop` = ≤K 跳可达后代（本工程 K=3，显式标注）
* `component_size` = 弱连通分量规模

> 旧实现里 `family_size = children_count + 1` 只是 **1 跳**，因此已改名为
> `direct_family_size`，避免与"真实家族规模"混淆。
>
> 节点数必须取 **(src ∪ dst) 去重并集**，不能把"不同底座数 + 不同衍生数"直接相加 ——
> 一个模型可以既是别人的底座、又是别人的衍生。实测相加为 975,365，去重后为
> 931,066，重复计入 44,299 个（Spark 原生统计与 GraphFrames 两个独立路径结果一致）。

### 7.5 RQ4 的研究定位与评估纪律

* 研究定位是**当前快照**下模型属性与下载热度的**统计关联**，不是"预测未来会不会火"。
* `y = log1p(downloads)`，`downloads` 为近 30 天口径。
* **先划分 train/test，再谈一切拟合**：预处理只在训练集 `fit`，指标只在**独立测试集**计算。
* `likes` 与 `downloads` 同取自一个快照，存在**同期相关**，不能包装成前瞻性预测能力。
* 源列 `update_interval_days` 实为 `lastModified − createdAt`（**维护跨度**），
  对外统一改名为 `maintenance_span_days`，不称"更新频率"。

### 7.6 RQ5 的可比性阈值与 `comparable` 语义

按 `pipeline_tag` 分组，`pipeline_tag` 缺失统一记 `unknown` 并**单独报告**（缺失率约 65.6%）。

`comparable` 的含义是「**有资格参与跨任务比较**」，而**不是**「样本量够大」：

* 只有 `n_models ≥ 200` **且** `distinct_values ≥ 20` 的**已标注**任务组才为 `true`；
* `unknown` 是任务标签缺失的**占位组**（205.8 万条），不是真实任务，
  **无论样本多大都恒为 `false`**，绝不参与任何跨任务排名与比较；
* `n_tasks_comparable` 只统计「已标注 且 `comparable`」的组。Spark 与 pandas
  两条路径口径现已完全一致（均为 **50**）；此前两条路径曾相差 1
  （Spark 先剔除 unknown 再计数，pandas 直接对全表求和），
  该分歧已由 `test_spark_contracts.py` 的断言锁定。

### 7.7 交付物完整性守卫（缺件必须报错，不能静默少画）

`make_figures.py` 的每个绘图分支都是 `if 数据存在:` 形式，一旦某个结果文件路径写错，
就会「少画一张图但退出码仍为 0」。因此 `main()` 末尾会核对 `FIG_INDEX`：
**12 张图缺任何一张即打印缺号并以退出码 1 结束**。本轮正是靠这条守卫发现了两类真实缺陷：

* `concentration_stats.json` 实际在 `gold/` 根目录而非 `gold/analysis/`，
  用 `load_json()`（指向 `analysis/`）会静默拿到 `None`，导致 Figure 5/6 被整段跳过、
  Dashboard 的 Gini / Top-1% / 零下载占比显示成 `-`。现已改用 `load_gold_json()`。
* Figure 11/12 选错规模档（见限制 14），导致 Figure 11 整张缺失、Figure 12 只画 1 个算子。

同类问题还有：ML 的四张表是 `df.write.csv()` 写出的**目录**（`part-*.csv` + `_SUCCESS`），
**不是**单个 `.csv` 文件，用 `pd.read_csv(目录)` 会失败并静默跳过 Figure 9 的 (a) 面板。
`make_figures.py` 因此新增 `read_spark_csv()` 逐 part 合并。

---

## 8. 已知限制（必须与结论一并陈述）

1. **研究对象是快照，不是完整历史**：本数据是某个固定时间点的公开模型仓库快照，
   RQ1 研究的是"快照中仍然可见模型的创建时间分布"，**不是**历史上所有曾发布模型的完整记录。
   已被删除/下架的历史模型不在数据里。
2. **2026-10 是不完整月份**：它是快照截止所在月，图中显式标注，不与其他完整月份直接比较。
3. **首月（2022-03）存在时间戳异常聚集（已观测现象）**：该月 32,621 个模型中 29,573 个集中在
   最早一天 `2022-03-02`，而该日只有 5 个不同时间戳、其中一个时间戳就占 21,869 个。
   这一聚集与逐日发布过程不符，因此**不能**解读为真实的单日发布高峰；
   但仅凭本快照**无法确认根因**（平台早期元数据迁移写入、时间戳记录下限等解释都缺少可核验证据），
   故**不作因果推断**。受此影响，首月数值不与其它月份直接比较。
4. **字段缺失率高**：`pipeline_tag` 65.56%、`library_name` 50.96%、`base_models` 71.74%、
   `gated` 100%（新数据源无该字段）。缺失**不做推断**，统一单独报告。
5. **存在自环边**：2,060 条边的 `src == dst`（模型把自己列为自己的 base model）。
   本工程**保留**这些边、不做静默删除，但它们会让节点单独构成规模为 1 的弱连通分量。
6. **外部父模型**：14,838 条边（1.61%）引用了快照外的底座模型（3,620 个不同父模型），
   这些边**被保留**，不静默丢弃。
7. **GraphFrames 的完成范围以结果文件为准**：`data_hubstats/gold/analysis/graphframes_result.json`
   的 `status` 与 `steps` 是唯一事实来源。本快照的实际结果是 `status = completed`，
   6 个步骤（度数统计 / connectedComponents / PageRank 正反向 / labelPropagation /
   逐节点指标合并）**全部在本机跑完**。报告中的相关叙述由该文件**自动生成**、
   **不写死**"某算法未完成"，以免出现"状态 completed 却写着有算法未完成"的自相矛盾。
   若将来重跑出现未完成步骤，会在同一处如实列出，**绝不用 NetworkX 结果冒充**。
8. **本机是 `local[N]` 单机多核，不是多节点集群**：Phase 3 结论只能说明本地并行度与
   资源对耗时的影响，**不得**表述为多节点横向扩展。
9. **OS 文件缓存无法清空**：Windows 无特权接口清 page cache，除
   `parquet_scan_first_touch` 外，所有 benchmark 计时都在缓存热态下取得。
10. **规模档位是"物化子集"，不是惰性过滤**：Spark 的 Parquet 向量化读取是
   "整批解码所需列、再应用过滤"，而 `pmod(crc32(id),100)<10` 属函数谓词、
   无法被 record-level filter 识别，于是过滤列以外的列**仍为整批行解码**。
   实测 10% 子集上强制解码全部 20 列要 5.411s，与 100% 全表的 5.096s 几乎相同。
   因此各档（10/25/50/100%）先物化成独立 Parquet 文件再计时
   （`data_hubstats/gold/analysis/bench_scale_subsets/`，约 2.9 GB，可复用），
   否则每个规模档都会白付一次全表扫描成本，"耗时随规模变化"的结论会失真。
   代价是子集的行组布局与原文件不同，这一差异已在报告中标注。
11. **Parquet 扫描算子使用不可下推的 `count(每列)`**：Spark 对 Parquet 的 `count(*)`
   会被**聚合下推到元数据层**、完全不解码数据列（实测全 20 列 `count()` 0.289s、
   6 列 0.600s，列数几乎没有影响）。若用 `count()` 比较"全表扫描 vs 列裁剪"，
   会得出虚假的"列裁剪没有收益"结论。改用不可下推的 `count(每列)` 后
   （全 20 列 4.857s vs 6 投影列 0.600s，约 8 倍）列裁剪收益才真实可测。
12. **Driver 内存取 3g，低于常规 8g 起步值**：本机物理内存 31.43 GiB，
   但实测**可用**内存只有约 6～8.5 GiB（用户自己的 qemu 虚拟机约 3.1 GiB
   与 Gradle/Kotlin 守护进程约 4.7 GiB 常驻占用）。实测取值窗口很窄：
   - `-Xmx2g`：`local[6]` 下 6 个并发任务解码全部 20 列时直接
     `java.lang.OutOfMemoryError: Java heap space`，实验无法完成；
   - `-Xmx3g`：可以完成，但若启动时可用内存只有约 6 GiB，
     JVM 的 RSS 峰值会达 6.18 GiB，把系统可用内存压到 0.004 GiB，
     100% 档重算子发生换页、耗时不可复现。
   因此 3g 是"能跑完"与"不压垮系统"之间唯一可用的取值，并配两重保护：
   (a) 编排器在每个阶段启动前打印可用内存，低于 7.5 GiB 时告警；
   (b) `benchmark.py` 对每次测量采样可用内存，低于 0.8 GiB 即把该行
   `mem_pressure` 置为 true 并**在汇总中单独成组**，绝不与正常测量混算中位数。
   被标记的行只在 `docs/benchmark_report.md` §3.2 中作为资源约束证据列出，
   **不参与任何性能对比**。
13. **shuffle/CPU 指标来自 Spark UI REST，取不到记"不可用"**：本地模式下
   StatusTracker 在 action 结束后取不到已完成 stage 的 taskMetrics，
   故改从 UI REST 读 stage 级累计值；同时**关闭 AQE**，因为 AQE 会把已物化的
   shuffle stage 标成 `SKIPPED`（指标恒为 0），会系统性低估 shuffle 量。
   确实取不到时 CSV 记**空值**并在汇总中排除，**绝不写 0**。
14. **规模档标签里有一个"哨兵档"，不能按行数最大值自动挑选**：P1 除 10/25/50/100% 四档外，
   还有一行 `real_cold_touch`（各档**物化之前**的首次冷读）。它的行数是 Bronze 全量
   3,138,959，比去重后的 `real_100pct`（3,138,954）**还多 5 行**，且只有 1 个分区档。
   若按"`n_rows` 最大"自动挑规模档就会选中它 —— 后果是 Figure 11 的分区对比变成空集、
   Figure 12 只画得出 1 个算子；报告侧还会因 `int("cold")` 直接抛 `ValueError`。
   现已统一按「**在至少有两个分区档的规模标签里取行数最大者**」挑选
   （`make_figures.py::top_scale_label` 与 `write_reports.py::scale_tier_pct`），
   并把冷读档在报告 §1 中作为**参考**单独列出、**不计入规模档对比**（冷读与缓存热态
   本就不可直接比较）。

---

## 9. 相关文档

| 文档 | 内容 |
| --- | --- |
| `docs/phase0_code_audit.md` | Phase 0 代码审计 |
| `docs/phase0_environment_audit.md` | Phase 0 环境审计 |
| `docs/phase0_execution_plan.md` | Phase 0 执行计划 |
| `docs/phase1a_environment_setup.md` | Phase 1A 环境搭建（含 winutils 结论） |
| `docs/phase1a_data_contract.md` | Phase 1A 数据契约 |
| `docs/phase1a_smoke_test.md` | Phase 1A 端到端冒烟测试 |
| `docs/full_etl_report.md` | 全量 ETL 报告 |
| `docs/data_quality_report.md` | 数据质量与守恒检查 |
| `docs/rq_analysis_report.md` | RQ1/RQ2 结果 |
| `docs/graph_analysis_report.md` | RQ3 结果（含 GraphFrames 完成情况） |
| `docs/ml_experiment_report.md` | RQ4 结果 |
| `docs/benchmark_report.md` | Phase 3 结果 |
| `docs/latex_report_materials.md` | LaTeX 报告素材（环境/数据/方法/结果/图对应） |
| `docs/ppt_handoff.md` | 8 分钟汇报结构与 Q&A |
| `docs/results_manifest.md` | **唯一**结果清单（人读版） |
| `docs/final_acceptance.md` | Phase 5 最终验收报告 |
| `docs/final_refinement_report.md` | 最终修复轮问题清单与验证结果 |
| `docs/visual_quality_review.md` | 12 张正式图视觉质量审查 |
| `docs/manual_review_checklist.md` | 人工语义复核清单（由 `check_consistency.py` 生成） |

---

## 10. 最终状态

最终状态由 `build_manifest.py` **依据证据推导**（而非手工断言），
写入 `results_manifest.json` 的 `final.status` / `final.status_reason`：

* `PROJECT READY FOR REPORT AND PRESENTATION`
* `PROJECT READY WITH LIMITATIONS`（会列出未完成的算法）
* `PROJECT NOT READY`（会列出硬性失败项）

以 `data_hubstats/gold/analysis/results_manifest.json` 的实际取值为准。
