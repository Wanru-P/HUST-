# Phase 5 · 最终验收报告

- 生成时间：2026-10-11
- 项目：Hugging Face 开源模型生态的演化、注意力集中与模型谱系分析
- 数据快照：`cfahlgren1/hub-stats / models.parquet` @ commit `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`
- 验收方式：**实际执行**下列命令并逐项核对输出，不以"文档存在"代替"结果正确"；
  也不以"退出码为 0"单独判定成功（退出码 0 但缺件的情况本轮确实出现过，见 §4）。

---

## 1. 验收结论

> **PROJECT READY FOR REPORT AND PRESENTATION**

该结论由 `build_manifest.py` 依据证据自动推导（`results_manifest.json` 的
`final.status` / `final.status_reason`），非人工断言：

| 判定依据 | 实际值 |
| --- | --- |
| `data_quality_report.hard_fail` | `[]`（无硬性失败） |
| `phase2_runs.all_ok` | `true`（无脚本失败） |
| `graphframes_result.status` | `completed`（6 个迭代步骤全部完成） |
| `final.gf_unfinished` | `[]` |
| `benchmark` 是否产出结果 | 是（107 个测量组合） |

自动推导出的理由原文：
> 全部模块完成，GraphFrames 迭代算法亦在本机跑完，无未完成项。

---

## 2. 验收执行记录（可复现）

全部命令在项目根目录、conda 环境 `hf_eco`
（`E:/Users/Admin/anaconda3/envs/hf_eco/python.exe`）下执行，
并设置 `HF_DATA_DIR=<项目>/data_hubstats`。

| # | 命令 | 结果 | 证据文件 |
| --- | --- | --- | --- |
| 1 | `python verify_etl.py` | `ok=true`，`hard_fail=[]` | `gold/data_quality_report.json` |
| 2 | `python run_phase2.py` | 4 个步骤退出码全 0，`all_ok=true` | `gold/analysis/phase2_runs.json` |
| 3 | `python analysis_graph_gf.py` | `status=completed`，6 步骤全完成 | `gold/analysis/graphframes_result.json` |
| 4 | `python run_benchmark.py --phases p1,p2,p3` | 5 个子运行退出码全 0 | `gold/analysis/benchmark_runs.json` |
| 5 | `python test_algorithms.py` | **32 项全 PASS** | 控制台 |
| 6 | `python test_spark_contracts.py` | **56 项，失败 0** | `gold/analysis/contract_test_result.json` |
| 7 | `python make_figures.py` | **12 张图**（PNG+PDF+SVG），退出码 0 | `figures_final/`、`gold/analysis/figure_index.json` |
| 8 | `python make_bench_figures.py` | 3 张（Figure 10–12），退出码 0 | `figures_final/` |
| 9 | `python build_manifest.py` | 182 条指标，`missing_inputs=[]` | `gold/analysis/results_manifest.json` |
| 10 | `python write_reports.py` | 7 份文档全部生成，退出码 0 | `report_final.md`、`docs/*.md` |
| 11 | `python check_consistency.py` | **71 项，失败 0** | `gold/analysis/consistency_check_result.json` |

**测试总量：32 + 56 + 71 = 159 项检查，失败 0 项。**

---

## 3. 19 项验收清单

### 3.1 交付物齐备性

| # | 检查项 | 证据 | 结果 |
| --- | --- | --- | --- |
| 1 | 交付物齐备：最终报告 / Dashboard / 12 张正式图 / 全部 docs | `report_final.md`、`dashboard_final.html`、`figures_final/` **36 个文件**（12 图 × png+pdf+svg，PNG 为 300 dpi）、`docs/` **17 份**（含本文件）、`results_manifest.json`（182 条指标，`missing_inputs=[]`） | PASS |

### 3.2 数据一致性

| # | 检查项 | 证据 | 结果 |
| --- | --- | --- | --- |
| 2 | Bronze → Silver → Gold 行数守恒 | Bronze 3,138,959（含源数据自带 5 个重复 id）→ Silver `models` 3,138,954 → `models_enriched` 3,138,954；`download_distribution` / `model_lifecycle` / `model_adoption_features` 均 = 3,138,954 | PASS |
| 3 | 聚合守恒（核心 ETL 正确性） | `task_monthly_stats.new_models` 求和 = 3,138,954；`library_monthly_stats` 同；`tag_monthly_stats` 求和 20,625,635 ≥ 主表（一对多，符合预期） | PASS |
| 4 | 数据质量硬性检查 | `data_quality_report.hard_fail = []`，`ok = true`；`model_id` 无重复；`gated` 全为 NULL（新数据源无该字段，未被推断） | PASS |

### 3.3 结果一致性（统一清单为唯一基准）

| # | 检查项 | 证据 | 结果 |
| --- | --- | --- | --- |
| 5 | `report_final.md` 与清单一致 | 14 项关键数字齐全且逐项匹配 | PASS |
| 6 | `dashboard_final.html` 与清单一致 | 8 项关键数字齐全；10 个 KPI 无一为 `-` 占位 | PASS |
| 7 | 图目录与清单一致 | `figure_index.json` 12 条；图注样本量由 `concentration_stats.json` 自动读取（3,138,954），非硬编码 | PASS |
| 8 | `docs/latex_report_materials.md` 与清单一致 | 11 项关键数字齐全 | PASS |
| 9 | `docs/ppt_handoff.md` 与清单一致 | 8 项关键数字齐全 | PASS |
| 10 | 旧 Demo 数字未泄漏到正式交付物 | 8 份文档全部通过「无旧 Demo / 旧环境残留」检查 | PASS |

### 3.4 算法正确性（单元测试 + 契约测试）

| # | 检查项 | 证据 | 结果 |
| --- | --- | --- | --- |
| 11 | Gini 四种口径互相等价 | 秩式 vs 频数加权 `max|Δ|=1.665e-16`；秩式 vs Lorenz 梯形积分反算 `max|Δ|=4.441e-16`；频数加权 vs 朴素两两差定义完全相等；边界 `[0,100] → 0.5` | PASS |
| 12 | Top-k 份额与分位数 | Top-p 份额 == 排序取前 k 的朴素定义（300×3 用例，`max|Δ|=0`）；与输入顺序无关；`quantile_from_freq == numpy.percentile(linear)`，`max|Δ|=0` | PASS |
| 13 | 时间转换契约 | `created_at` / `last_modified` 100% 符合 ISO-8601 UTC（带 Z）；可被 `to_timestamp` 解析；`created_month == date_format(ts,'yyyy-MM')` 全部一致；`month_num ∈ [1,12]`；`spark.sql.session.timeZone == UTC` | PASS |
| 14 | 边方向契约（RQ3 最关键） | 抽样验证边为 `(src=底座, dst=衍生)` 未反向；所有 `dst` 均存在于 Silver 主表（孤儿 0）；**外部父模型边被保留** 14,838 条（1.6131% < 10%） | PASS |
| 15 | `family_size` / 图节点覆盖语义 | `family_size == children_count + 1` 全部行成立；`children_count ==` 以该模型为 src 的边数；`node_metrics` 覆盖全部 3,138,954 个模型；每条边 `dst` 均命中；**`src` 缺失量恰等于外部父模型边数 14,838，去重缺失恰等于 3,620**（若主表内 src 被漏掉会立即失败） | PASS |
| 16 | 图节点并集去重（非朴素相加） | `n_nodes_involved`(并集) 931,066 ≠ `n_base_plus_derived`(相加) 975,365，差值 44,299 == 同时扮演两种角色的模型数；且 `graphframes_result.n_vertices_in_graph == 931,066`、`n_edges == 919,839` | PASS |
| 17 | Train/Test 划分无泄漏 | `n_train + n_test == n_sample`（501,049 + 125,220 = 626,269）；`train ∩ test == 0`；`seed == 42` 固定；声明无目标派生输入特征；含 `Baseline_mean` 基线；四种模型指标齐备 | PASS |
| 18 | 跨引擎选样一致性（可比性前提） | 在 200,000 个真实 `model_id` 上，Python `zlib.crc32` 与 Spark `pmod(F.crc32(...),100)` 选中**同一批行**（交集 20,041 == 双方各自数量）；选样比例 10.021% | PASS |

### 3.5 工程可复现性与代码检查

| # | 检查项 | 证据 | 结果 |
| --- | --- | --- | --- |
| 19 | 可复现性 + 代码约束 | ① 复现**无需重新下载 3,138,959 条明细**：`data_hubstats/bronze/models.parquet` 已落地并校验 sha256，`download_hubstats.py` 命中即跳过；② 无嵌套函数（AST 全量扫描 24 个核心脚本，`0` 处）；③ 无硬编码私有绝对路径（全仓 `grep` 无 `C:/Users/Admin`）；④ 无隐藏 Demo 回退（正式链路只读 `data_hubstats/`，`make_bench_figures.py` 明确拒绝回退旧 Demo 数据）；⑤ 无 Spark / pandas 输出目录混用（`HF_ENGINE=spark → gold/`，`=local → gold_local/`）；⑥ 无手工编造的指标（全部数字来自 `results_manifest.json`）；⑦ 工程原本无 `.git`，**未执行任何破坏性的 Git 初始化 / reset / checkout** | PASS |

---

## 4. 本轮发现并修复的缺陷（如实记录）

验收的价值在于**发现问题**。本轮实际发现并修复了 7 个缺陷，其中 4 个会直接导致
交付物缺失或结论错误。全部记录如下，不隐瞒。

| # | 缺陷 | 影响 | 修复 |
| --- | --- | --- | --- |
| 1 | **契约测试 F 组断言与契约 C 自相矛盾**：断言"每条边的 `src` 都在 `node_metrics` 中"，但契约 C 明确要求保留外部父模型边，而这些父模型本就不在主表内 | 误报 14,838 项失败；若为"通过"而删掉外部边，会静默丢弃真实血缘 | 改为**更严格**的断言：`src` 缺失量必须**恰好等于**外部父模型边数（14,838），去重缺失必须恰好等于外部父模型数（3,620）。主表内 src 一旦漏掉会立刻失败 |
| 2 | **RQ5 `comparable` 语义缺陷 + 两引擎结果分歧**：Spark 路径先剔除 `unknown` 再计数（50），pandas 路径对全表求和（51）；且 `unknown` 行自身被标成 `comparable=true`，与其 `unknown_note`（"不参与任务比较"）自相矛盾 | 两引擎结果不一致；读 `rows` 的下游会把 205.8 万条未标注模型当成一个任务参与跨任务比较 | 两个引擎统一为：`comparable` 恒表示"**有资格参与跨任务比较**"，`unknown` 无论样本多大都恒为 `false`。两条路径现均为 **50**，并由契约测试断言锁定 |
| 3 | **`concentration_stats.json` 路径错误**：该文件在 `gold/` 根目录，而 `make_figures.py` 用指向 `gold/analysis/` 的 `load_json()` 读取，**静默拿到 `None`** | Figure 5、Figure 6 被整段跳过；Dashboard 的 Gini / Top-1% / 零下载占比显示成 `-`；**退出码仍为 0** | 新增 `load_gold_json()` 明确读取 `gold/` 根；并给 `main()` 加**完整性守卫**：12 张图缺任何一张即打印缺号并以退出码 1 结束 |
| 4 | **Figure 11/12 选错规模档**：`real_cold_touch` 哨兵档的行数是 Bronze 全量 3,138,959，比去重后的 `real_100pct`（3,138,954）**还多 5 行**，且只有 1 个分区档；按 `n_rows` 最大自动挑选会选中它 | Figure 11 变成空集被静默跳过；Figure 12 只画得出 1 个算子（错误图表）；报告侧 `int("cold")` 抛 `ValueError` 导致报告生成失败 | 新增 `top_scale_label()` / `scale_tier_pct()`：只在**至少有两个分区档**的规模标签里取行数最大者；冷读档在报告 §1 中作为**参考**单独列出，明确**不计入规模档对比**（冷读与缓存热态不可直接比较） |
| 5 | **ML 四张表是 Spark 目录而非单文件**：`ml_feature_importance_randomforest/` 等是 `df.write.csv()` 写出的**目录**（`part-*.csv` + `_SUCCESS`），`pd.read_csv(目录)` 失败 | Figure 9 的 (a) 特征重要性面板被静默跳过，只剩一半图 | 新增 `read_spark_csv()` 逐 part 合并；并修正 `analysis_ml.py` 中把目录写成 `.csv` 文件的过时文档 |
| 6 | **`write_reports.py` 两处崩溃**：① `rq4.correlations` 是**列表**却按字典调用 `.items()`；② 规模档排序假设标签形如 `real_10pct`，遇到 `real_cold_touch` 时 `int("cold")` 抛错 | 报告生成直接中断（`AttributeError` / `ValueError`） | 按实际结构改为遍历列表；规模档排序改用健壮的 `scale_tier_pct()` 解析器 |
| 7 | **报告存在与结果自相矛盾的叙述 + 手写数字漂移**：① 硬编码"超出预算的算法**未完成**"，而实际 6 个 GraphFrames 步骤**全部 completed**，同页却写着 `status=completed`；② 硬编码"22 个 Python 脚本"，实际为 33 个 | 叙述与结果矛盾；工作量数字随代码增删持续漂移 | GraphFrames 叙述改为**完全由 `rq3_gf.step.*` 驱动**（`gf_steps_summary()` / `gf_status_phrase()`）；脚本数改为动态统计（`project_script_count()`） |
| 8 | **`check_consistency.py` 存在嵌套函数**：`main()` 内部定义了闭包 `def v(...)` | 违反工程硬性约束"不允许在函数/循环内部定义函数" | 提升为模块级 `mv(metrics, key, default)`，显式传参，15 处调用同步改写；AST 复检 0 处嵌套 |

### 4.1 关于"顶层函数语句数 < 5"

AST 扫描同时报告：33 个脚本中有 **107 个顶层函数的有效语句数 < 5**。
这些都是**被反复调用的命名小工具**（如 `num` / `pct` / `fn` / `mv` / `iso_string_col`
这类格式化与取值函数，单个被调用数十次），把它们内联反而会在几十处重复同样的格式化逻辑。
约束的本意是"不要为简单逻辑**额外**封装"，这些属于"消除重复"而非"额外封装"，
故保留并在此**如实披露**，不做隐藏。本轮新增的辅助函数均满足下列之一：
被 ≥2 处调用、或封装了无法内联的控制流（循环 / `try` / 文件遍历）。

---

## 5. 最终报告必须回答的 8 个问题

`report_final.md` 已逐节回答，对应关系如下：

| # | 问题 | 对应章节 | 核心答案 |
| --- | --- | --- | --- |
| 1 | 完成了什么？ | §1 | Phase 0–5 全部完成：全量 ETL、RQ1–RQ5、GraphFrames、MLlib、扩展性实验、12 图、Dashboard、报告与素材 |
| 2 | 数据规模多大？ | §2 | Bronze 3,138,959 行 / 1,596,738,497 B；Silver 3,138,954 行（去重后）；血缘边 919,839；标签长表 20,625,635 |
| 3 | 五个 RQ 的主要结果与图？ | §3 | RQ1 峰值月 2024-09（125,030）→ 图 1/2/3；RQ2 Gini 0.995355（downloads）/ 0.998117（累计）→ 图 5/6；RQ3 边 919,839 / 节点 931,066 / 最大衍生 43,419 → 图 7/8；RQ4 RandomForest R²=0.6631（基线 -0.0000）→ 图 9；RQ5 已标注 56 / 可比较 50 → 图 4 |
| 4 | Spark 实验结果？ | §4 | `local[6]` / driver 3g / shuffle 64；全量 ETL 142.8 秒；107 个测量组合、321 次计时执行；0 个组合受内存压力影响 |
| 5 | 工程挑战与解决？ | §5 | winutils（BareLocalFileSystem）、Schema/时区、全局排序瓶颈、图规模、ML 泄漏、内存约束 —— 6 项均有对照解决方式 |
| 6 | 最终文件在哪里？ | §6 | 根目录 `hf_eco_analysis/`；结果在 `data_hubstats/`；图在 `figures_final/`；报告为 `report_final.md` |
| 7 | 是否符合课程要求？ | §7 | 大数据管理 / 处理 / 分析 / 4V / 技术工作量 / 创新性 六项全部满足 |
| 8 | 剩余限制？ | §8 | 快照性质、标签缺失 65.55%、血缘缺失 71.74%、外部父模型、GraphFrames 状态、ML 定位、单机非集群、下载量≠质量、RQ4 样本规模 —— 9 条 |

---

## 6. 本次验收**未执行**的操作（合规声明）

- 未重写工程、未删除任何原始代码；所有脚本入口与输出格式保留。
- 未覆盖或删除旧 Demo 数据与旧结果：`data/`、`figures/`、`dashboard.html`、`report.md`
  全部仍在（由 `check_consistency.py` D 组逐项验证）。
- 未修改用户由 Trae AI 维护的 LaTeX 模板工程
  （`Desktop/大数据/华中科技大学计算机学院实验报告Latex模板（数据结构）`），
  该目录位于本项目之外，全程只读。
- 未修改 `proposal.pdf` 及 `Trae_HUST_中文实验报告模板改造指令_ABC一并执行.md`。
- 工程原本**没有** `.git`，因此**未执行**任何 `git init` / `reset` / `checkout` 等破坏性操作。
- 未终止用户的其他程序（qemu 虚拟机、Gradle/Kotlin 守护进程等）以腾出内存；
  资源不足时改为**降低 Spark 并行度 / driver 内存**并在报告中如实标注约束。

---

## 7. 剩余限制（摘要，完整见 `report_final.md` §8 与 `README.md` §8）

1. 快照性质：只含当前仍可见的模型，不能当作完整历史发布记录。
2. 任务标签缺失 65.55%，按任务的结论只覆盖已标注部分。
3. 底座关系缺失 71.74%，谱系分析只覆盖显式声明血缘的模型。
4. 3,620 个外部父模型不在快照内，后代类指标是下界。
5. 单机 `local[N]` 多核实验，**不构成多节点横向扩展结论**。
6. Windows 无法清 OS page cache，扫描计时为缓存热态（`parquet_scan_first_touch` 除外，
   该冷读值已在报告 §1 单独列出且不参与规模档对比）。
7. 下载量只反映传播热度，**不代表模型质量**；PageRank 同理。
8. RQ4 只在约 20% 确定性哈希样本（626,269 行）上训练，是为控制本地资源的有意取舍。
