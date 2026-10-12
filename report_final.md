# Hugging Face 开源模型生态分析 · 最终报告

- 生成时间：2026-10-11T12:19:59.105477+00:00
- 数据快照：`cfahlgren1/hub-stats / models.parquet` @ commit `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`
- 正式样本：快照内全部 3,138,954 个去重模型（源文件物理行 3,138,959，其中 5 个 id 在源数据中重复出现）
- 全部数字来自唯一结果清单 `data_hubstats/gold/analysis/results_manifest.json`，不手抄、不估算；其中 RQ2 / RQ3 / RQ4 的补充验证数字（三口径 Gini、零下载年龄构成、衍生-下载秩相关、时间切分 R²）与数据质量补充（负维护跨度）来自 `data_hubstats/gold/analysis/supplementary_results.json`


---

## 1. 完成情况

### 1.1 已完成的模块

| 阶段 | 内容 | 状态 |
| --- | --- | --- |
| Phase 0 | 工程与本地环境审计、新数据源核实、修改计划 | 已完成 |
| Phase 1A | PySpark 环境搭建、Parquet 接入、Schema 适配、5 万条端到端测试 | 已完成 |
| Phase 1B | 全量 3,138,959 行 Spark ETL + 数据质量与守恒验证 | 已完成 |
| Phase 2 | RQ1–RQ5 正式分析（含 GraphFrames 迭代算法、MLlib 独立测试集） | 已完成 |
| Phase 3 | 真实数据 Spark 扩展性实验 + 单独标注的合成数据压力测试 | 已完成 |
| Phase 4 | 12 张正式图、Dashboard、报告、LaTeX / PPT 素材 | 已完成 |
| Phase 5 | 数据/结果/算法/工程一致性验收 | 已完成 |

### 1.2 完成状态与受限部分（如实说明）

- **无未完成模块**：GraphFrames 迭代算法状态 `completed`，全部 6 个步骤均在 32 GB 本机、`local[6]` 条件下跑完 —— 度数统计 3.9 秒、connectedComponents 352.9 秒、PageRank（正向） 375.0 秒、PageRank（反向） 348.9 秒、labelPropagation 215.0 秒、逐节点指标合并 13.6 秒。（本机预算 2,400 秒，未触发看门狗。）
- **过程与结论要分清三层**：
  1. **初始风险**：迭代类图算法在 93 万节点 / 92 万边上开销高，存在超出本机预算的可能（GraphX 在 20 节点图上跑 connectedComponents 就要 46 秒）；
  2. **应对策略**：把迭代算法拆到独立脚本、加超时看门狗、每个步骤单独落盘以便续跑，并对同快照已落盘结果做复用；
  3. **最终状态**：全部步骤实际完成。**完成 ≠ 没有代价** —— 单步仍需数百秒，内存与时间开销是真实约束，不能因为最终跑通就描述成「轻松完成」。
- RQ3 的图统计因此同时具备两条独立证据：GraphFrames 迭代结果（连通分量 / 双向 PageRank / 标签传播）与 Spark 原生精确统计（出度/入度/直接家族规模/≤K 跳可达后代/relation 分布/外部父模型），完整边表未抽样。

### 1.3 关键设计取舍

- 不重写工程、不覆盖旧 Demo 数据与旧结果；新正式产物与旧 Demo 物理隔离。
- 不虚构任何实验结果；所有数字可追溯到 `results_manifest.json` 中的出处。
- 不把 Trending Top-N 样本当作全平台总体：正式分析使用全量 3,138,954 行。

---

## 2. 数据规模

| 层 | 内容 | 行数 | 磁盘 |
| --- | --- | --- | --- |
| Bronze | 源 `models.parquet`（只读，未修改） | 3,138,959 | 1,596,738,497 B |
| Bronze | 其中源数据自带的重复 id | 5 | — |
| Silver | `models` 主表（去重后） | 3,138,954 | 458,545,882 B（silver 全目录） |
| Silver | `model_tags` 标签长表 | 20,625,635 | — |
| Silver | `model_base_edges` 血缘边表 | 919,839 | — |
| Gold | `models_enriched` 宽表 | 3,138,954 | 697,384,662 B（gold 全目录） |

Gold 各表行数：

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

---

## 3. 五个研究问题的主要结果

### RQ1 生态演化

- 核心指标：峰值月 **2024-09**，新增 **125,030** 个；时间范围 `2022-03-02T23:29:04.000Z` → `2026-10-10T13:13:07.000Z`。
- 任务标签缺失 2,057,742 个（65.55%），单独报告，未推断。
- 对应图：**Figure 1 / 2 / 3**。
- 限制①：2026-10 未走完；研究对象是快照可见模型的创建时间分布。
- 限制②（时间戳异常聚集）：首月 `2022-03` 有 90.7% 的模型挤在 `2022-03-02` 一天，而该日仅 5 个不同时间戳（众数时间戳占 73.9%）。这是**已观测到的现象**，不能当作自然发布高峰；**根因未确认**（元数据迁移 / 时间戳记录下限等解释均缺可核验证据），故不作因果推断。

### RQ2 注意力集中与长尾

- Gini（downloads，近 30 天）= **0.995355**；Top-1% 份额 **97.13%**；零下载占比 **43.54%**。
- Gini（downloadsAllTime，累计）= **0.998117**；Top-1% 份额 **98.70%**。
- 两种口径合计下载量相差约 27.2 倍，**必须声明口径，不可混用、不可相加**。
- **不宣称幂律**：只描述长尾形态，未做严格分布拟合与模型比较。
- **稳健性（补充验证）**：剔除 43.54% 的零下载模型后，Gini 仅从 0.995355 降至 **0.991773**（累计口径从 0.998117 降至 0.996754），Top-1% 内部 Gini **0.936576** —— 下载量高度集中的现象**并不只是由零下载记录造成的**；零下载中 97.2% 创建已超 30 天（其 30 天下载窗口已完整，不属新模型的时间伪影），剔除新模型后零下载率 43.74% 与原值几乎相同。详见图 Figure S1 与 `docs/supplementary_analysis.md`。
- 对应图：**Figure 5 / 6**（及 Figure S1）。

### RQ3 模型谱系网络

- 边语义：`src = 底座` → `dst = 衍生模型`（方向保留）。
- 血缘边 **919,839** 条；底座 **88,333** 个；衍生 **887,032** 个；节点（并集去重）**931,066** 个。
- 最大直接衍生数 **43,419**（`black-forest-labs/FLUX.1-dev`）。
- 外部父模型边 **14,838** 条（1.6131%），**完整保留**。
- GraphFrames 状态：`completed`（6 个迭代步骤全部完成）；连通分量在**图内**为 42,890 个（把图外孤立模型也算上的全量为 2,250,778 个），labelPropagation 得到 126,431 个社区，PageRank 正向与反向均已计算。
- 四类规模指标严格区分：`children_count`（出度）/ `direct_family_size`（1 跳直系）/ `descendant_count_khop`（有界可达）/ `component_size`（弱连通分量）。
- PageRank 方向明确：正向高分是衍生链末端，反向高分才是「被依赖中心」；**PageRank 不等于模型质量**。
- **衍生热度 ≠ 下载热度（补充验证）**：底座模型的衍生数（`children_count`）与**其自身**下载量的秩相关仅 **+0.37**（累计口径 **+0.39**；log 尺度 Pearson +0.46 / +0.48；n = 84,713）——属中等正相关，**既不是同一个量，也非相互独立**。两榜头部**无一重叠**（前十交集为 0；收窄到同一总体、都在快照内底座内排序，交集同样为 0），且类别构成相反：被大量微调的是**大型生成式底座**（FLUX.1-dev 43,419 衍生 / 66.2 万下载），被大量下载的是**小型句子嵌入/编码模型**（all-MiniLM-L6-v2 仅 1,280 衍生 / **2.256 亿**下载）。据此**不能把下载量读作被依赖程度**（反之亦然）。详见 `docs/supplementary_analysis.md` Part D。
- 对应图：**Figure 7 / 8**。

### RQ4 属性与下载热度的统计关联

- 定位：当前快照下的**统计关联**，不是前瞻性预测。
- 样本 626,269 行（19.95% 确定性哈希），训练 501,049 / 测试 125,220，train∩test = 0。
- 目标 `y = log(1+downloads)`；预处理只在训练集上 fit；指标只在测试集上算。

| 模型 | RMSE | MAE | R² |
| --- | --- | --- | --- |
| Baseline_mean | 1.7467 | 1.4033 | -0.0000 |
| LinearRegression | 1.3328 | 0.9501 | 0.4177 |
| RandomForest | 1.0139 | 0.5876 | 0.6631 |
| GBT | 1.0606 | 0.6239 | 0.6313 |

- **时间外推验证（补充）**：以 sklearn 复现同一特征与建模样本（随机切分 RandomForest R²=0.68，对应上表 Spark 口径 0.6631），仅把切分改为按 `createdAt` 时间切分（最老 80% 训练 / 最新 20% 测试）后，RandomForest R² 降至 **0.50（−26%）**，GBT 亦从 0.62 降至 0.47（−24%）。该差值说明**模型性能对创建时间队列的分布差异较为敏感**；由于特征与目标均取自同一快照，**不能**把 −26% 直接归因为「时间泄漏的贡献」（队列分布变化、训练样本构成等同样可能贡献该差值），也不代表未来热度预测能力。时间切分的 0.50 仍显著优于基线（−0.03），说明特征携带可跨队列泛化的信号。likes 的同期相关见 `docs/supplementary_analysis.md` Part C（该分层对照未发现单调衰减，但也不足以证明同期相关不构成影响）。详见图 Figure S2。
- **特征数据质量（补充验证）**：RQ4 所用特征 `maintenance_span_days`（= lastModified − createdAt）存在 **28,097 个负值（0.895%）**，最小值 **−20,041 天**（约 −55 年，物理上不可能），且该特征 **83.8% 的整日法取值为 0**（即 0 ≤ Δ < 1 天，**不是**「同一日历日期」；精确 Δ==0 仅 423,856 个）；负值分布不均（`fill-mask` 9.27%、`kernels` 26.05%）。该项**只量化、不臆断根因**（与 §8 第 1 项对 2022-03 异常同口径）。详见 `docs/supplementary_analysis.md` Part E。
- 对应图：**Figure 9**（及 Figure S2）。

### RQ5 任务生态对比

- 已标注任务 56 个，可比较 50 个（最小样本阈值 200）。
- unknown 任务单独成行报告（2,057,742 个模型），不参与跨任务比较。
- 两种下载口径独立成列，不混合、不相加。
- 对应图：**Figure 4**。

---

## 4. Spark 实验与工程结果

| 项 | 值 |
| --- | --- |
| Spark Master | local[6] |
| Driver Memory | 3g |
| shuffle.partitions | 64 |
| 全量 ETL 端到端墙钟耗时 | 142.8 秒 |
| ETL 退出码 | 0 |
| JVM RSS 峰值 | 3.67 GiB |
| 系统可用内存最低值 | 3.87 GiB |
| 真实数据 benchmark 测量次数 | 107 |
| 规模扫描组 Spark Master | local[6] |
| 并行度扫描档位 | local[2], local[4], local[6] |

ETL 各阶段耗时（**脚本内计时**，起点为 SparkSession 就绪）：

| 阶段 | 累计秒数 |
| --- | --- |
| SparkSession 就绪 (master=local[6], driver=3g) | 0.0 |
| Silver 三表写出完成 | 45.3 |
| models_enriched 写出并回读完成 (n=3138954) | 73.5 |
| Gold 三张月度统计表写出完成 | 86.5 |
| download_distribution 写出完成 | 106.9 |
| lorenz_points 写出完成 | 110.2 |
| 全量 ETL 结束 | 120.6 |

⚠️ **两个 ETL 计时口径不同，不可互相替代**：

- **端到端墙钟 142.8 秒**：由 `run_full_etl.py` 在**启动子进程之前**开始计时，覆盖进程启动、Spark 会话建立、ETL 计算与进程退出；
- **脚本内阶段累计 120.6 秒**：由 `spark_etl.py` 自己的计时器给出，其 t0 取在 **SparkSession 就绪之后**，只覆盖 ETL 计算本身；
- 两者相差约 **22.2 秒**，主要包含脚本内计时范围之外的启动、会话建立和退出等开销，此处不声称已逐项精确测定。**两个数字都是真实测量值，不为了「统一」而改动其中任何一个。**

### 主要瓶颈

1. **shuffle 主导的算子**（groupBy / join / graph_edges）随规模增长最快；
2. **全局排序**：原下载量分布统计使用无 `partitionBy` 的全局窗口，把 314 万行压到单个分区串行处理；已改为频数聚合 + 组内并行排名，并与旧实现做了逐行等价对拍；
3. **重复扫描**：原实现每张 Gold 表都从 Bronze 重算（约 10 次全量扫描）；改为 Bronze 只读一次后落盘回读，样本 ETL 从 193 秒降到 50.7 秒；
4. **数据倾斜**：少数底座模型（如最大者被 43,419 个模型继承）在按底座聚合时会产生热点分区。
5. **迭代图算法的固定开销**：GraphX 在 20 节点图上 connectedComponents 就要 46 秒，固定开销极大 —— 这是**最初判断「全量图迭代算法可能在本机跑不完」的主要原因**。实际通过独立脚本 + 超时看门狗 + 逐步骤落盘续跑，最终 6 个迭代步骤全部完成；但单步仍需数百秒，开销本身并未消失。

---

## 5. 工程挑战与解决

| 挑战 | 表现 | 解决方式 |
| --- | --- | --- |
| Windows 上 Spark 缺 winutils.exe | `RawLocalFileSystem.setPermission` 在 Windows 只有原生库或 shell 两条分支，`core-default.xml` 无开关可关闭 | 反编译确认无法用配置绕过，改用纯 Java 的 `BareLocalFileSystem`（Apache-2.0，已做源码与字节码审计），放入 `<pyspark>/jars/` |
| Schema 适配 | 新数据源 `createdAt` 读成 `timestamp_ntz`（不是 `TimestampType`），`baseModels` 是 `struct{relation, models:[{_id,id}]}` | `schema_adapter.py` 统一列名与类型；`iso_string_col` 同时处理两种时间类型；`transform(baseModels.models, x -> x.id)` 提取父模型 id |
| 时区 | `spark.sql.session.timeZone` 默认为本地时区，ISO-Z 字符串被按东八区渲染，23:xx 的 UTC 记录被推到次月 | 全局固定 `session.timeZone=UTC`，并用契约测试验证 `created_month` 一致 |
| 全局排序瓶颈 | 无 `partitionBy` 的 Window 触发「Moving all data to a single partition」 | 改为按下载量取值聚合出频数表（唯一值数比模型数小三个数量级），再按取值查表 + 组内序号还原排名，语义逐行等价 |
| 图规模与迭代算法 | 97 万节点 / 92 万边的图上，迭代算法开销巨大 | 拆分脚本：Spark 原生精确统计必完成；GraphFrames 单独跑 + 超时看门狗 + 逐步骤落盘可续跑（本次 6 个迭代步骤全部完成）；NetworkX 只做小规模验证与可视化 |
| ML 评估泄漏 | 原实现在全量上 fit 又在同一数据上评估 | 先划分 train/test，预处理只在训练集 fit，只在测试集评估，并补简单基线 |
| 内存约束 | 物理 32 GB，实际可用远低于 32 GB | 从 `local[4]` 起步做并行度扫描；driver 内存按需调整而非无上限加大；每次长计算前检查可用内存与磁盘 |
| 原始时间戳质量 | `28,097` 条记录的 `lastModified` 早于 `createdAt`（0.895%，最小 −20,041 天），物理上不可能；另有 83.836% 的记录整日法差值为 0（0 ≤ Δ < 1 天，**不是**「同一日历日期」） | 作为 RQ4 特征的已知数据质量限制**如实披露**：量化规模与分布、**不臆断根因**（与 2022-03 时间戳异常同口径处理）；`supplementary/maintenance_span_sanity.py` 可复现 |

---

## 6. 最终文件位置

| 交付物 | 相对路径 |
| --- | --- |
| 项目根目录 | `hf_eco_analysis/` |
| 正式数据（Bronze/Silver/Gold） | `data_hubstats/{bronze,silver,gold}/` |
| 分析结果 | `data_hubstats/gold/analysis/` |
| 结果清单（唯一基准） | `data_hubstats/gold/analysis/results_manifest.json` |
| 正式图（PNG/PDF/SVG） | `figures_final/` |
| Dashboard | `dashboard_final.html` |
| 最终报告 | `report_final.md` |
| 分析报告 | `docs/rq_analysis_report.md`、`docs/graph_analysis_report.md`、`docs/ml_experiment_report.md`、`docs/benchmark_report.md` |
| 数据质量报告 | `docs/data_quality_report.md` |
| ETL 报告 | `docs/full_etl_report.md` |
| LaTeX 素材 | `docs/latex_report_materials.md` |
| PPT 素材 | `docs/ppt_handoff.md` |
| 验收报告 | `docs/final_acceptance.md` |
| 结果清单表格 | `docs/results_manifest.md` |
| 运行日志 | `data_hubstats/_smoke/*.log`、`data_hubstats/_smoke/*.json` |
| 旧 Demo（保留，未改动） | `data/`、`figures/`、`dashboard.html`、`report.md` |

---

## 7. 是否符合《大数据导论》课程要求

| 要求 | 对应实现 | 是否满足 |
| --- | --- | --- |
| 大数据管理 | Medallion 三层架构（Bronze/Silver/Gold）+ 7 张分析表 + 集中度统计表；Schema 契约与数据质量守恒检查 | 满足 |
| 大数据处理 | PySpark 4.2.0 全量 ETL、Explode/GroupBy/Join/Window、GraphFrames 迭代算法、MLlib 分布式训练 | 满足 |
| 大数据分析 | RQ1–RQ5 五类分析：时间演化、集中度与长尾、图谱系、回归关联、分组对比 | 满足 |
| 4V | Volume：3,138,959 行 / 1.49 GiB；Variety：数值 + 嵌套 struct + 数组；Velocity：近 30 天下载窗口 + 滚动快照；Veracity：缺失率显式量化、不推断 | 满足 |
| 技术工作量 | 34 个 Python 脚本；完整执行顺序与日志；扩展性实验 107 个测量组合、共 321 次计时执行（每组另有 1 次不计时预热） | 满足 |
| 创新性 | ① 频数加权 Gini 的精确口径与逐行等价改造；② 边语义严格保留的谱系分析（含外部父模型）；③ GraphFrames 与 Spark 原生双轨、逐步骤落盘可续跑，并如实记录每一步的真实耗时（本次无未完成步骤，若有则同样如实记录）；④ 单一结果清单驱动全部交付物，杜绝数字漂移 | 满足 |

---

## 8. 剩余限制

1. **快照性质**：数据只含当前仍公开可见的模型，已删除/下架的历史模型缺失，早期月份绝对量系统性偏低；因此不能把创建时间统计当作完整历史发布记录。此外首月 `2022-03` 存在时间戳异常聚集（`2022-03-02` 单日占首月 90.7%，该日仅 5 个不同时间戳）——现象已观测确认，**根因未确认、不作因果推断**，该月数值不可与其它月份直接比较。
2. **任务标签缺失 65.55%**：统一记为 unknown，按任务的结论只覆盖已标注部分，不能据此宣称「某任务主导整个生态」。
3. **底座关系缺失 71.74%**：谱系分析只覆盖显式声明血缘的模型；未声明血缘不等于没有血缘。
4. **外部父模型**：3,620 个父模型不在快照内，跨样本谱系在快照边界被截断，后代类指标是下界。
5. **GraphFrames 的计算代价（已克服，但代价真实）**：状态 `completed`（6 个迭代步骤全部完成）。初始风险是迭代算法在 93 万节点 / 92 万边上开销高；应对策略是独立脚本 + 超时看门狗 + 逐步骤落盘可续跑；最终全部完成。残余约束是单次全量迭代仍需数百秒，且**未用 NetworkX 结果替代**。
6. **ML 的定位**：RQ4 是当前快照下的统计关联，不是前瞻预测；时间切分后 R²≈0.50（随机切分 0.68，−26%）只说明模型性能对创建时间队列的分布差异敏感，**不能**归因为「时间泄漏贡献」——特征与目标同快照，该实验不具备量化泄漏幅度的条件（见 §3.4）；likes 的同期相关问题见 `docs/supplementary_analysis.md` Part C（分层对照未发现单调衰减，但不足以证明同期相关不构成影响）；特征重要性不代表因果。
7. **单机实验**：全部实验在 `local[N]` 单机多核完成，**不构成多节点横向扩展结论**；Windows 无法清 OS 文件缓存，扫描计时为缓存热态（首次接触除外）。
8. **下载量不等于质量**：本报告全文避免用「质量」描述下载热度，也不把 PageRank 等同于质量。
9. **课程规模限制**：RQ4 只在 20% 确定性哈希样本上训练，不是全量训练；这是为控制本地资源的有意取舍，已在报告中说明。
10. **`maintenance_span_days` 含不可能值**：该 RQ4 特征有 **28,097 个负值（0.895%）**（最小 −20,041 天，约 −55 年，物理上不可能），且 **83.8% 的整日法取值为 0**（0 ≤ Δ < 1 天，**不是**「同日」；精确 Δ==0 仅 423,856 个）；负值在任务/库间分布不均（`fill-mask` 9.27%、`kernels` 26.05%）。这是时间戳记录/口径问题，**只量化、不臆断根因**（与本条第 1 项 2022-03 异常同口径）。详见 `docs/supplementary_analysis.md` Part E。

---

## 9. 最终状态

**PROJECT READY FOR REPORT AND PRESENTATION**

全部模块完成，GraphFrames 迭代算法亦在本机跑完，无未完成项。


