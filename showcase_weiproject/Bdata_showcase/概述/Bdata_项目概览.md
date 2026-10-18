# Bdata 文件夹 · 重要数据与关键步骤概览

> 本文档是对 `c:\Users\wxt07\Desktop\Bdata` 全目录扫描后整理出的快速导读，涵盖项目定位、目录结构、重要数据规模、五个研究问题的核心结论、完整执行流程、关键技术挑战与交付物清单。
> 所有数字均摘自项目内的权威结果清单 `data_hubstats/gold/analysis/results_manifest.json` 及各摘要 JSON，未做任何估算或手抄修改。

---

## 1. 项目定位（一句话看懂这是什么）

这是一个《大数据导论》课程项目（对应 **Proposal 2**），题目为：

> **《开源人工智能模型生态的演化、注意力集中与模型谱系分析》**

它以 **Hugging Face Hub 的公开模型快照**为研究对象，使用 **PySpark 4.2.0 本地模式**（Windows 11）完成从数据落地、Medallion 分层 ETL、图算法、机器学习到可视化的可复现分析。

---

## 2. 顶层目录结构

```
Bdata/
├── data_hubstats/              # 数据指纹与轻量结果摘要（不含原始/中间大数据）
│   ├── bronze/                 # 原始数据指纹（ingest_manifest.json）
│   └── gold/                   # 结果层
│       ├── analysis/           # RQ1–RQ5、GraphFrames、Benchmark、结果清单
│       ├── concentration_stats.json
│       └── data_quality_report.json
├── docs/                       # 方法、结果、质量、展示与验收文档（20+ 篇）
├── figures_final/              # Fig01–Fig12（每张 PNG + PDF + SVG 三格式）
├── jars/                       # Windows 本地 Spark 所需的额外 JVM 构件
├── *.py                        # 下载、ETL、分析、绘图、验证脚本（34 个）
├── dashboard_final.html        # 离线结果看板（打开时不运行 Spark）
├── dashboard_standalone.html   # 单文件分享版看板（图片 base64 内嵌，4.9 MB）
├── report_final.md             # 最终研究报告
├── requirements.txt            # Python 依赖清单
└── README.md                   # 项目入口说明
```

> 说明：本公开交付包**不包含**约 1.49 GiB 的原始模型快照、约 5.8 GB 的 Spark 中间表/日志、缓存和历史备份（见 `.gitignore`）。这些可由脚本按数据源重新获取；必要的轻量结果摘要均已保留。

---

## 3. 数据源与数据规模（重要数据）

### 3.1 数据源指纹（可校验）

| 项 | 值 |
| --- | --- |
| 数据集 | Hugging Face `cfahlgren1/hub-stats` |
| 文件 | `models.parquet` |
| 固定提交 commit | `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70` |
| 源文件 SHA-256 | `d86485a02a7aace1b8c4eda4a849dd153a702d5351ab5a57f3c2320176e251ef` |
| 源文件大小 | 1,596,738,497 字节（约 1.49 GiB） |
| 下载镜像 | `hf-mirror.com`（本机直连 huggingface.co 不通） |
| 校验结果 | `ingest_manifest.json` 中 `sha256_match: true` |

### 3.2 Medallion 三层数据规模

| 层 | 内容 | 行数 | 磁盘 |
| --- | --- | --- | --- |
| Bronze | 源 `models.parquet`（只读） | 3,138,959 | 1,596,738,497 B |
| Bronze | 源数据自带的重复 id | 5 | — |
| Silver | `models` 主表（去重后） | 3,138,954 | 458,545,882 B |
| Silver | `model_tags` 标签长表 | 20,625,635 | — |
| Silver | `model_base_edges` 血缘边表 | 919,839 | — |
| Gold | `models_enriched` 宽表 | 3,138,954 | 697,384,662 B |

**Gold 各分析表行数：**

| 表 | 行数 |
| --- | --- |
| download_distribution | 3,138,954 |
| library_monthly_stats | 7,915 |
| lorenz_points | 62,763 |
| model_adoption_features | 3,138,954 |
| model_family_stats | 575 |
| model_lifecycle | 3,138,954 |
| tag_monthly_stats | 861,394 |
| task_monthly_stats | 2,587 |

**时间范围：** `2022-03-02` → `2026-10-10`，共 56 个月。

**每年模型数（创建时间分布）：**

| 年份 | 模型数 |
| --- | --- |
| 2022 | 100,392 |
| 2023 | 318,206 |
| 2024 | 740,836 |
| 2025 | 1,103,162 |
| 2026（截至 10 月） | 876,358 |

**字段缺失率（显式量化，未推断）：**

| 字段 | 缺失率 |
| --- | --- |
| pipeline_tag（任务标签） | 65.56% |
| library_name（库名） | 50.96% |
| base_models（底座关系） | 71.74% |
| gated | 100%（新数据源无该字段） |

---

## 4. 技术栈与运行环境

| 项 | 值 |
| --- | --- |
| 操作系统 | Windows 11（Build 26200） |
| CPU | Intel Core Ultra 7 255HX（20 逻辑核） |
| 物理内存 | 31.43 GiB |
| Java | OpenJDK 17 |
| Python | 3.11.17（conda 环境 `hf_eco`） |
| 核心引擎 | PySpark 4.2.0（内置 Hadoop 3.5.0 / Scala 2.13 / GraphX） |
| 图算法 | GraphFrames 0.12.3 |
| 分析出图 | pandas 3.0.6 / pyarrow 26.0.0 / matplotlib 3.11.2 / networkx 3.6.1 |
| 资源监控 | psutil 7.2.2 |
| Spark 运行模式 | `local[6]`，driver 内存 4g，shuffle 分区 64 |

**额外 JVM 构件（`jars/`，不能靠 pip 安装，必须放入 `<pyspark>/jars/`）：**

| jar | 用途 |
| --- | --- |
| `hadoop-bare-naked-local-fs-0.1.0.jar` | 纯 Java 本地文件系统，绕开 winutils.exe |
| `graphframes-spark4_2.13-0.12.3.jar` | GraphFrames 主构件 |
| `graphframes-graphx-spark4_2.13-0.12.3.jar` | GraphFrames 的 GraphX 后端 |

---

## 5. 五个研究问题（RQ）与核心结论

### RQ1 — 生态如何随时间演化

- 峰值月 **2024-09**，新增 **125,030** 个模型。
- 时间范围 `2022-03-02` → `2026-10-10`（56 个月；2026-10 为快照截止月，未走完，不可与其他完整月比较）。
- 任务标签缺失 **2,057,742** 个（65.55%），统一记 `unknown` 单独报告，未推断成任何具体任务。
- Top 任务（按模型数）：`text-generation` 422,224、`text-classification` 123,924、`text-to-image` 111,444、`reinforcement-learning` 78,390、`image-text-to-text` 41,815 等。
- **时间戳异常聚集（重要限制）**：首月 2022-03 有 90.7% 的模型挤在 `2022-03-02` 一天，而该日仅 5 个不同时间戳（众数时间戳占 73.9%）。这是**已观测到的现象**，根因未确认，不作因果推断。
- 对应图：**Figure 1 / 2 / 3**。

### RQ2 — 下载注意力的集中程度与长尾

两种下载口径**必须严格区分**：

| 口径 | 合计下载量 | Gini | Top-1% 份额 | 零下载占比 |
| --- | --- | --- | --- | --- |
| `downloads`（近 30 天） | 2,788,919,780 | **0.995355** | 97.13% | 43.54% |
| `downloadsAllTime`（历史累计） | 75,949,397,771 | **0.998117** | 98.70% | 41.98% |

- 两种口径合计下载量相差约 **27.2 倍**，任何结论都必须声明口径，不可混用、不可相加。
- Gini 用**频数加权秩公式**精确计算（非采样近似），三种独立口径经 `test_algorithms.py` 验证互相等价（最大偏差 ≤ 4.4e-16）。
- **不宣称幂律**：只描述长尾形态，未做严格幂律拟合与模型比较。
- 对应图：**Figure 5 / 6**。

### RQ3 — 模型谱系网络（底座 → 衍生）

- 边语义：`src = 底座` → `dst = 衍生模型`（方向不可反）。
- 血缘边 **919,839** 条；底座 88,333 个；衍生 887,032 个；节点（并集去重）**931,066** 个。
- 最大直接衍生数 **43,419**（`black-forest-labs/FLUX.1-dev`）。
- 外部父模型边 **14,838** 条（1.6131%），共 3,620 个不在快照内的父模型，**完整保留**。
- 血缘关系分布：`finetune` 354,320、`adapter` 329,707、`quantized` 187,784、`merge` 48,028。
- Top 底座（按直接衍生数）：FLUX.1-dev(43,419)、Qwen1.5-0.5B(32,534)、Qwen1.5-1.8B(30,626)、gemma-2b(24,036)、distilbert-base-uncased(13,182) 等。
- GraphFrames 迭代算法状态 `completed`（6 个步骤全部完成）：度数统计 3.9s、connectedComponents 352.9s（图内 42,890 个连通分量，含孤立模型全量为 2,250,778 个）、PageRank 正向 375.0s、PageRank 反向 348.9s、labelPropagation 215.0s（126,431 个社区）、逐节点指标合并 13.6s。
- 对应图：**Figure 7 / 8**。

### RQ4 — 模型属性与下载热度的统计关联（非预测）

- 研究定位：当前快照下的**统计关联**，不是"预测未来会不会火"。
- 目标：`y = log1p(downloads)`（近 30 天口径）。
- 采样：确定性哈希 `crc32(model_id) % 5 == 0`，得到 626,269 行（19.95%）。
- 划分：`randomSplit([0.8, 0.2], seed=42)`，训练 501,049 / 测试 125,220，**train∩test=0**，预处理只在训练集 fit，指标只在测试集计算。
- 测试集 R²：Baseline(≈0)、LinearRegression 0.418、RandomForest **0.663**、GBT 0.631。
- 特征重要性（RandomForest）：`tag_count` 0.413 > `library_top` 0.194 > `likes_log` 0.151 > `task` 0.133 > 其余。
- 重要纪律：`likes` 与 `downloads` 同取自一个快照，存在**同期相关**，重要性不代表因果。
- 对应图：**Figure 9**。

### RQ5 — 不同任务生态的结构差异

- 按 `pipeline_tag` 分组，56 个已标注任务，其中 50 个满足可比性（`n_models ≥ 200` 且唯一取值数 ≥ 20）。
- `unknown`（任务标签缺失的占位组，2,057,742 条）恒不参与跨任务比较。
- 各任务组独立计算下载 Gini、Top-k 份额、零下载占比、模型年龄、底座覆盖率、家族规模等指标。
- 对应图：**Figure 4**。

---

## 6. 关键步骤（完整执行顺序）

所有命令在项目根目录执行，`PY` 指代 conda 环境解释器 `E:/Users/Admin/anaconda3/envs/hf_eco/python.exe`：

```bash
# ⓪ 一次性：把额外 jar 装进 <pyspark>/jars/（幂等，可重复执行）
python setup_spark_fs.py

# ① 数据落地（若 bronze/models.parquet 已存在且 sha256 一致可跳过）
python download_hubstats.py

# ② Bronze -> Silver -> Gold 全量 ETL（3,138,959 行）
$env:HF_ENGINE='spark'; $env:HF_SPARK_MASTER='local[6]';
$env:HF_SPARK_DRIVER_MEMORY='4g'; $env:HF_SHUFFLE_PARTITIONS=64
python run_full_etl.py

# ③ 全量数据质量与守恒检查 -> data_quality_report.json
python verify_etl.py

# ④ Phase 2：RQ1/RQ2 -> RQ3 -> RQ4 -> RQ5（顺序固定，前者产物是后者输入）
python run_phase2.py
#    失败重跑单个步骤：python run_phase2.py --only ml,rq5

# ⑤ RQ3 的 GraphFrames 迭代算法（长任务，带超时看门狗，可断点续跑）
python analysis_graph_gf.py

# ⑥ Phase 3：Spark 扩展性实验（规模扫描 + 并行度扫描 + 合成压力测试）
python run_benchmark.py                 # 全部三组
python run_benchmark.py --phases p1     # 只跑真实数据规模扫描

# ⑦ Phase 5：数据契约与算法正确性测试
python test_algorithms.py               # 纯 Python 算法等价性（不需要 Spark）
python test_spark_contracts.py          # Spark 侧契约（A–H 组，需真实数据）

# ⑧ Phase 4：12 张正式图 + Dashboard
python make_figures.py                  # -> figures_final/ 与 dashboard_final.html

# ⑨ 结果清单 + 报告 + 一致性检查（顺序不可颠倒）
python build_manifest.py                # -> results_manifest.json（唯一结果基准）
python write_reports.py                 # -> 7 份报告（全部从清单读取）
python check_consistency.py             # 校验数字一致、无旧 Demo 残留等 114 项检查
```

**关键环境变量：**

| 变量 | 默认值 | 作用 |
| --- | --- | --- |
| `HF_DATA_DIR` | `data_hubstats` | 数据根目录 |
| `HF_ENGINE` | `spark` | `spark`→silver/gold；`local`→silver_local/gold_local |
| `HF_SPARK_MASTER` | `local[6]` | Spark master |
| `HF_SPARK_DRIVER_MEMORY` | `3g` | driver 内存 |
| `HF_SHUFFLE_PARTITIONS` | `64` | shuffle 分区数 |
| `HF_REACH_HOPS` | `3` | RQ3 可达后代的 K 跳上限 |
| `HF_GF_BUDGET_SEC` | `2400` | GraphFrames 看门狗预算 |

**引擎/结果隔离原则：** Spark 与 local（pandas/networkx 等价路径）两套引擎产物落在不同目录，互不覆盖；旧 Demo 的 `data/`、`figures/`、`dashboard.html`、`report.md` 全程只读、从未改动。

---

## 7. 关键技术挑战与解决（重要经验）

| 挑战 | 表现 | 解决方式 |
| --- | --- | --- |
| Windows 缺 winutils.exe | `RawLocalFileSystem.setPermission` 无配置可绕过 | 改用纯 Java 的 `BareLocalFileSystem`（Apache-2.0），放入 `<pyspark>/jars/` |
| Schema 适配 | 新数据源 `createdAt` 是 `timestamp_ntz`，`baseModels` 是嵌套 struct | `schema_adapter.py` 统一列名与类型 |
| 时区 | ISO-Z 时间被按本地时区渲染，23:xx UTC 被推到次月 | 全局固定 `session.timeZone=UTC` 并契约测试验证 |
| 全局排序瓶颈 | 无 `partitionBy` 的 Window 触发单分区 shuffle | 改为按下载量取值聚合出频数表，再查表 + 组内序号还原排名 |
| 图规模与迭代算法 | 93 万节点 / 92 万边迭代开销巨大 | 拆分脚本 + 超时看门狗 + 逐步骤落盘可续跑 |
| ML 评估泄漏 | 全量 fit 又在同一数据评估 | 先划分 train/test，预处理只在 train fit，只在 test 评估 |
| 内存约束 | 物理 32 GB，实际可用远低于 32 GB | 从 `local[4]` 起步并行度扫描，按需调整 driver 内存 |

---

## 8. 重要交付物清单

### 8.1 核心结果文件

| 交付物 | 路径 |
| --- | --- |
| 唯一结果清单（所有数字的基准） | `data_hubstats/gold/analysis/results_manifest.json`（约 354 KB） |
| 集中度统计 | `data_hubstats/gold/concentration_stats.json` |
| 数据质量报告 | `data_hubstats/gold/data_quality_report.json` |
| 谱系统计 | `gold/analysis/graph_stats.json`、`graphframes_result.json` |
| ML 结果 | `gold/analysis/ml_summary.json` |
| RQ5 结果 | `gold/analysis/rq5_summary.json`、`rq5_task_ecosystem.csv` |
| 一致性检查 | `gold/analysis/consistency_check_result.json`（114 项，0 失败） |
| 契约测试 | `gold/analysis/contract_test_result.json`（56 项，0 失败） |
| 算法等价性 | `gini_equivalence_result.json`（最大偏差 ≤ 4.4e-16） |

### 8.2 正式图表（figures_final/，每张三格式）

| 图 | 主题 |
| --- | --- |
| Figure 1 | 月度新增模型数演化 |
| Figure 2 | 任务构成演化（堆叠面积图） |
| Figure 3 | Task × 年份热力图 |
| Figure 4 | 库与任务生态结构对比 |
| Figure 5 | 下载量 Lorenz 曲线（双口径） |
| Figure 6 | 下载量长尾与头部集中度（CCDF） |
| Figure 7 | 模型谱系结构（全量统计 + 家族子图） |
| Figure 8 | 家族规模与连通分量规模分布 |
| Figure 9 | 特征重要性与模型对比 |
| Figure 10 | 执行时间 vs 数据规模 |
| Figure 11 | shuffle 分区数 vs 性能 |
| Figure 12 | 算子耗时与 shuffle 量对比 |

### 8.3 文档（docs/，20+ 篇）

| 文档 | 内容 |
| --- | --- |
| `FULL_REPRODUCTION_GUIDE.md` | 完整环境配置与全量复现说明（**唯一复现入口**） |
| `data_quality_report.md` | 数据质量报告 |
| `full_etl_report.md` | ETL 报告 |
| `rq_analysis_report.md` / `graph_analysis_report.md` / `ml_experiment_report.md` / `benchmark_report.md` | 各研究问题分析报告 |
| `latex_report_materials.md` / `ppt_handoff.md` | LaTeX / PPT 素材 |
| `final_acceptance.md` | 验收报告 |
| `results_manifest.md` | 结果清单表格 |
| `phase0_*` / `phase1a_*` | 环境审计、数据契约、冒烟测试等阶段文档 |

---

## 9. 剩余限制（如实声明，读结论时必须知晓）

1. **快照性质**：只含当前仍公开可见的模型，已删除/下架的历史模型缺失，早期月份绝对量系统性偏低。
2. **任务标签缺失 65.55%**：按任务的结论只覆盖已标注部分。
3. **底座关系缺失 71.74%**：谱系分析只覆盖显式声明血缘的模型。
4. **外部父模型**：3,620 个父模型不在快照内，后代类指标是下界。
5. **GraphFrames 计算代价真实**：单次全量迭代仍需数百秒，未用 NetworkX 结果替代。
6. **ML 定位**：RQ4 是同期统计关联，不是前瞻预测。
7. **单机实验**：全部在 `local[N]` 单机多核完成，不构成多节点横向扩展结论。
8. **下载量 ≠ 质量**：全文避免用"质量"描述下载热度。

---

## 10. 30 秒快速总结

- **是什么**：用 PySpark 分析 Hugging Face 开源模型生态（演化、注意力集中、谱系、任务差异）的大数据课程项目。
- **数据量**：约 1.49 GiB 的 Parquet 快照，314 万行去重模型、2063 万标签、92 万血缘边。
- **架构**：Medallion 三层（Bronze/Silver/Gold）+ 7 张分析表 + 唯一结果清单驱动全部交付物。
- **核心结论**：生态 2024 年 9 月达峰值；下载注意力极度集中（Gini ≈ 0.995–0.998）；谱系形成以 FLUX.1-dev 等底座为中心的巨型家族；标签数量与库选择是下载热度的最强关联特征。
- **工程亮点**：winutils 绕行、UTC 锁定、频数加权 Gini、GraphFrames 可续跑、ML 防泄漏、114 项一致性检查全通过。

---

*文档生成时间：2026-10-12 · 基于对 Bdata 目录的全量扫描与核心结果文件读取。*
