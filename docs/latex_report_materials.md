# LaTeX 报告素材（供 Trae AI 中文 LaTeX 模板使用）

- 生成时间：2026-10-11T12:19:59.059991+00:00
- 数据快照：`cfahlgren1/hub-stats / models.parquet` @ commit `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`
- 正式样本：快照内全部 3,138,954 个去重模型（源文件物理行 3,138,959，其中 5 个 id 在源数据中重复出现）
- 全部数字来自唯一结果清单 `data_hubstats/gold/analysis/results_manifest.json`，不手抄、不估算
- 本文件只提供**内容与数字**；模板工程由用户用 Trae AI 维护，本项目不修改该模板

> 使用说明：所有数字均已给出且可追溯到结果清单。请直接取用，**不要另行估算或改动**；口径说明（脚注/表注）必须一并保留。

## 1. 环境描述

| 项 | 值 | 出处 |
| --- | --- | --- |
| 操作系统 | Windows 11（Build 26200） | docs/phase0_environment_audit.md |
| CPU / 内存 | Intel Core Ultra 7 255HX / 32 GB 物理内存 | docs/phase0_environment_audit.md |
| Java | OpenJDK 17（`E:/Software/jdk-17`） | docs/phase1a_environment_setup.md |
| Python | 3.11（conda 环境 `hf_eco`，与 base 隔离） | docs/phase1a_environment_setup.md |
| PySpark | 4.2.0（内置 Hadoop 3.5.0） | docs/phase1a_environment_setup.md |
| GraphFrames | 0.12.3（JVM 构件已 SHA-1 校验） | schema_adapter.py |
| Spark Master（ETL） | local[6] | `full_etl_run_report.json:config` |
| Driver Memory（ETL） | 3g | `full_etl_run_report.json:config` |
| shuffle.partitions（ETL） | 64 | `full_etl_run_report.json:config` |
| 会话时区 | `spark.sql.session.timeZone = UTC`（否则 ISO-Z 会按东八区平移月份） | docs/phase1a_environment_setup.md |
| Windows 文件系统 | 用纯 Java 的 `BareLocalFileSystem` 替代 winutils.exe（无需原生库） | docs/phase1a_environment_setup.md |

## 2. 数据源与数据特征

| 项 | 值 |
| --- | --- |
| 数据源 | Hugging Face Hub 公开数据集 `cfahlgren1/hub-stats` 的 `models.parquet` |
| 快照 commit | `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70` |
| 文件 SHA-256 | `d86485a02a7aace1b8c4eda4a849dd153a702d5351ab5a57f3c2320176e251ef` |
| 文件大小 | 1,596,738,497 字节（约 1.49 GiB） |
| 源物理行数 | 3,138,959 |
| 源重复 id 数 | 5（去重后为 3,138,954 行，属于正确去重而非数据丢失） |
| 字段数 | 69 个叶子列（含嵌套 `baseModels`） |
| 创建时间范围 | `2022-03-02T23:29:04.000Z` → `2026-10-10T13:13:07.000Z`（UTC） |
| 关键缺失率 | model_id 0.00%；author 0.00%；created_at 0.00%；last_modified 0.00%；downloads 0.00%；downloadsAllTime 0.00%；likes 0.00%；pipeline_tag 65.55%；library_name 50.96%；tags 0.00%；gated 100.00%；base_models 71.74%；base_relation 71.74% |

## 3. 技术架构（Bronze → Silver → Gold）

```
data_hubstats/bronze/models.parquet   原始快照（只读，不覆盖）
        │  PySpark 读取 + Schema 适配（时间统一 ISO-8601 UTC；baseModels 展开）
        ▼
data_hubstats/silver/  models | model_tags | model_base_edges
        │  派生字段 + 月度聚合 + 集中度统计
        ▼
data_hubstats/gold/    models_enriched + 七张分析表 + lorenz_points
                       + concentration_stats.json
        │  分析层：RQ1/RQ2、RQ3(Spark 原生 + GraphFrames)、RQ4(MLlib)、RQ5
        ▼
data_hubstats/gold/analysis/  全部正式结果（CSV / JSON / parquet）
        │  只读这些文件（不重跑 Spark）
        ▼
figures_final/ + dashboard_final.html + report_final.md
```

| 阶段 | 行数 | 磁盘占用 |
| --- | --- | --- |
| Bronze（源） | 3,138,959 | 1,596,738,497 B |
| Silver（主表） | 3,138,954 | 458,545,882 B（silver 全目录） |
| Gold（宽表 + 七表 + 集中度） | 3,138,954 | 697,384,662 B（gold 全目录） |
| 血缘边表 | 919,839 | — |
| 标签长表 | 20,625,635 | — |

## 4. 4V 特征

| 维度 | 在本数据上的体现 | 证据 |
| --- | --- | --- |
| Volume（体量） | 源快照 3,138,959 行 / 1.49 GiB，展开后标签长表 20,625,635 行，血缘边 919,839 条 | `data_quality_report.json` |
| Variety（多样） | 结构化数值（下载/点赞）+ 半结构化嵌套（`baseModels` struct）+ 数组型标签；57 种任务、2,708 种库、424,435 个不同标签 | `data_quality_report.json` |
| Velocity（时效） | 快照为滚动更新；下载量字段本身是近 30 天窗口计数，天然带时效性；创建时间跨度约 4.6 年 | `summary_rq1.json` |
| Veracity（真实性） | 任务标签缺失 65.55%、底座关系缺失 71.74%；源数据含 5 个重复 id；全部缺失均显式保留、不推断 | `data_quality_report.json` |

## 5. RQ1–RQ5 方法与结果

### RQ1 生态演化

- 方法：按 `created_month` 聚合；研究对象为「快照可见模型的创建时间分布」。
- 结果：峰值月 2024-09（125,030 个）；任务标签缺失 65.55%，单独报告。
- 图：Figure 1 / 2 / 3。详见 `docs/rq_analysis_report.md`。

### RQ2 注意力集中与长尾

- 方法：频数加权秩公式精确计算 Gini；Top-p 用明确排名定义；Lorenz 用真实累计点。
- 结果（downloads 口径）：Gini = **0.995355**，Top-1% 份额 97.13%，零下载占比 43.54%。
- 结果（downloadsAllTime 口径）：Gini = **0.998117**，Top-1% 份额 98.70%。
- **不宣称幂律**：只描述长尾形态，未做分布拟合与模型比较。
- 图：Figure 5 / 6。

### RQ3 模型谱系网络

- 方法：边语义固定 `base → derived`；Spark 原生算精确图统计，GraphFrames 跑迭代算法。
- 结果：边 919,839 条，底座 88,333 个，衍生 887,032 个，节点（并集）931,066 个；最大直接衍生数 43,419。
- 外部父模型边 14,838 条（1.6131%），全部保留。
- GraphFrames 状态：`completed`（6 个迭代步骤全部完成）。
- 图：Figure 7 / 8。详见 `docs/graph_analysis_report.md`。

### RQ4 属性与下载热度的统计关联

- 方法：先划分 train/test（seed=42），预处理只在训练集上 fit，指标只在测试集上算；目标 `y = log(1+downloads)`。
- Baseline_mean：RMSE 1.7467，MAE 1.4033，R² -0.0000
- LinearRegression：RMSE 1.3328，MAE 0.9501，R² 0.4177
- RandomForest：RMSE 1.0139，MAE 0.5876，R² 0.6631
- GBT：RMSE 1.0606，MAE 0.6239，R² 0.6313
- **定位声明**：这是当前快照下的统计关联，不是前瞻性预测；likes 与 downloads 同期相关，不能当作预测能力。
- 图：Figure 9。详见 `docs/ml_experiment_report.md`。

### RQ5 任务生态对比

- 方法：按 `pipeline_tag` 分组，两种下载口径独立成列，最小样本阈值 200。
- 结果：已标注任务 56 个，其中可比较（样本充足）50 个；unknown 单独报告。
- 图：Figure 4。详见 `docs/rq_analysis_report.md`。

## 6. Spark 扩展性实验（Phase 3）

| 项 | 值 |
| --- | --- |
| Spark Master | local[6] |
| Driver Memory | 3g |
| 规模档位 | real_100pct, real_10pct, real_25pct, real_50pct, real_cold_touch |
| 对应行数 | 314,455, 785,597, 1,569,149, 3,138,959 |
| 重复与统计量 | 1 次预热 + 3 次计时，报告中位数 |
| 真实数据 ETL 端到端墙钟耗时 | 142.8 秒（含 SparkSession 启动；脚本内阶段累计见 report_final.md §4） |
| JVM RSS 峰值 | 3.67 GiB |
| 系统可用内存最低值 | 3.87 GiB |
| 测量精度（离散度中位数） | 12.5%（47 组，1 次预热 + 3 次计时） |
| 列裁剪效果（全列解码 vs 6 投影列，real_100pct） | 4.891 s vs 0.567 s，约 8.6 倍 |
| 全列解码的规模效应 | real_10pct 0.797 s → real_100pct 4.891 s |
| shuffle 最重的算子（real_100pct） | etl_pipeline 5.105 s，shuffle 读 33.21 MiB |
| 本地并行度档位（同一规模、同一算子） | local[2], local[4], local[6] |

- 图：Figure 10 / 11 / 12。详见 `docs/benchmark_report.md`。
- 结论要点：耗时随规模单调增长；shuffle 主导算子增长最快；分区数存在最优点而非越大越好；**列裁剪显著降低扫描成本**（但必须用不解码列的方法测不出来）；全局排序与数据倾斜是主要风险点（已在 Phase 1B 修复）。

## 7. 关键指标定义（表注可直接引用）

| 指标 | 定义 |
| --- | --- |
| `downloads` | 近 30 天下载计数（窗口口径） |
| `downloadsAllTime` | 历史累计下载计数（累计口径）；与前者不可混用 |
| Gini | `G = 2S/(n·total) − 1`，`S = Σ(rank−0.5)·x`；等价频数加权形式 `S = Σ vᵢfᵢ(F_{i−1}+Fᵢ)/2`，精确值非近似 |
| Top-p 份额 | 按值降序取前 `max(1, int(n·p))` 个模型的下载量占比；并列值整体同权 |
| `children_count` | 出度：被多少模型直接当作底座 |
| `direct_family_size` | `children_count + 1`，只是 1 跳直系规模 |
| `descendant_count_khop` | ≤3 跳可达后代数（有界，非全量后代） |
| `component_size` | 弱连通分量规模，与家族规模含义不同 |
| `maintenance_span_days` | `lastModified − createdAt`，是**维护跨度**，不是更新频率 |
| `label` | `log(1 + downloads)` |

## 8. 图表对应关系

| 图 | 文件 | 标题 | 数据源 |
| --- | --- | --- | --- |
| Figure 1 | `figures_final/fig01_monthly_evolution.png` | 月度新增模型数（快照可见模型的创建时间分布） | gold/analysis/monthly_new_models.csv, gold/analysis/summary_rq1.json |
| Figure 2 | `figures_final/fig02_task_growth.png` | 任务构成演化（堆叠面积图） | gold/analysis/pipeline_trend.csv, gold/analysis/summary_rq1.json |
| Figure 3 | `figures_final/fig03_task_time_heatmap.png` | Task × 年份热力图 | gold/analysis/pipeline_trend.csv |
| Figure 4 | `figures_final/fig04_library_task_structure.png` | 库与任务生态结构对比 | gold/analysis/library_trend.csv, gold/analysis/rq5_task_ecosystem.csv |
| Figure 5 | `figures_final/fig05_lorenz.png` | 下载量 Lorenz 曲线（双口径） | gold/analysis/lorenz_curve.csv, gold/concentration_stats.json |
| Figure 6 | `figures_final/fig06_ccdf_longtail.png` | 下载量长尾与头部集中度 | gold/analysis/ccdf.csv, gold/concentration_stats.json |
| Figure 7 | `figures_final/fig07_family_network.png` | 模型谱系结构：全量统计 + 代表性家族子图 | gold/analysis/results_manifest.json, silver/model_base_edges, gold/analysis/node_metrics, gold/analysis/graphframes_result.json |
| Figure 8 | `figures_final/fig08_family_size.png` | 家族规模与连通分量规模分布 | gold/model_family_stats, gold/analysis/component_size_dist.csv, gold/analysis/graph_stats.json |
| Figure 9 | `figures_final/fig09_ml_importance.png` | 特征重要性与模型对比 | gold/analysis/ml_feature_importance_randomforest.csv, gold/analysis/ml_summary.json |
| Figure 10 | `figures_final/fig10_scale_runtime.png` | 执行时间 vs 数据规模 | gold/analysis/benchmark_all_summary.csv |
| Figure 11 | `figures_final/fig11_partitions_perf.png` | shuffle 分区数 vs 性能 | gold/analysis/benchmark_all_summary.csv |
| Figure 12 | `figures_final/fig12_operator_comparison.png` | 算子耗时与 shuffle 量对比 | gold/analysis/benchmark_all_summary.csv |

每张图同时提供 PNG（300 dpi，位图）与 PDF / SVG（矢量，适合 LaTeX 插入）。

## 9. 数据限制（必须在报告中声明）

- 快照性质：只含**当前仍公开可见**的模型；已删除/下架的历史模型不在数据内，早期月份绝对量系统性偏低。
- 任务标签缺失 65.55%，统一记为 unknown，按任务的结论只覆盖已标注部分。
- 底座关系缺失 71.74%，谱系分析只覆盖显式声明血缘的那部分模型。
- 外部父模型 3,620 个不在快照内，跨样本谱系在快照边界被截断，后代类指标是下界。
- `2026-10` 为快照截止月，未走完，不与完整月份直接比较。
- **首月时间戳异常聚集**：`2022-03` 有 90.7% 的模型挤在 `2022-03-02` 一天，该日仅 5 个不同时间戳（众数时间戳占 73.9%）。这是**已观测到的现象**，**根因未确认**（平台元数据迁移 / 时间戳记录下限等解释均缺少可核验证据），故不作因果推断，也不当作自然发布高峰。
- 下载量只反映传播热度，**不代表模型质量**；本报告不使用「质量」描述下载热度。
- 相关性不等于因果；特征重要性不等于因果。
- 全部实验在 `local[N]` 单机多核上完成，**不构成多节点横向扩展的结论**。

### 口径纪律（易混淆项，严禁互相替代）

| 易混淆的两侧 | 正确表述 |
| --- | --- |
| 下载热度 / 模型质量 | 下载量只反映传播热度，**不等于质量** |
| PageRank / 模型质量 | PageRank 只反映图结构中的被引用强度，**不等于质量**，也不代表技术水平 |
| 同期统计关联 / 前瞻性预测 | RQ4 是同一快照内的同期关联，**不是**对未来热度的预测 |
| 快照创建时间分布 / 历史完整发布记录 | 数据只含当前仍可见的模型，**不是**历史上全部发布记录 |
| 任务标签缺失模型 / 已标注任务 | `unknown`（缺失占位）**不是一个任务类别**，不得计入按任务的结论 |
| `children_count` / `direct_family_size` | 后者 = 前者 + 1（含自身）；二者都是 **1 跳直系**规模 |
| `component_size` / 家族规模 | 弱连通分量跨多跳、含外部引用，**不等于** 1 跳家族规模 |
| `descendant_count_khop` / 全量后代 | 只统计 ≤3 跳，**是下界**，不是全量后代 |
| `maintenance_span_days` / 更新频率 | `lastModified − createdAt` 是**维护跨度**，不是更新频率 |

