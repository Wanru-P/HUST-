# 最终轻量修复报告

> 生成时间：2026-10-11 19:46 CST  
> 对应轮次：Phase 5 之后的轻量修复（Tasks A–C）  
> 核心原则：**优先修改生成器源码，不手动改自动产物。**

---

## 1. 本轮目标与限制

本轮只修复人工检查发现的 3 个问题，**不重新采集数据、不重新执行全量 Spark ETL、不重新执行 GraphFrames 迭代算法、不重新训练 MLlib、不重新运行 Spark Benchmark、不修改任何正式实验指标、不修改 Trae 维护的 LaTeX 工程、不编写 LaTeX 报告正文。**

| 任务 | 问题 | 修改策略 |
| --- | --- | --- |
| A | Figure 7 在 A4 单栏报告中可读性不足 | 在 `make_figures.py` 新增独立的报告版导出逻辑，生成 `figures_report/fig07a_*` 与 `figures_report/fig07b_*` |
| B | 上一轮报告把历史通过记录与本轮实际执行混为一谈 | 更新 `docs/final_refinement_report.md`，明确区分历史 Spark 契约测试通过记录与本轮实际执行范围 |
| C | `report_final.md` 中出现 `3138954.0` 等计数被错误格式化为浮点数 | 在 `write_reports.py` 修复显示调用点，新增 `fmt_count()` 辅助函数，并重新生成所有受影响文档 |

---

## 2. 实际修改的源码文件

| 文件 | 修改内容 |
| --- | --- |
| `make_figures.py` | 1. 新增 `FIG_REPORT_DIR` 与 `save_report()`；<br>2. 新增 `fig07a_report_global_lineage()`：2×2 紧凑统计卡片，读自 `results_manifest.json`；<br>3. 新增 `fig07b_report_representative_family()`：单个底座 hub-and-spoke 子图，边来自 Silver 边表；<br>4. 给 `draw_family_subgraph()` 增加 `label_width` 参数，避免报告版标签过长；<br>5. 在 `main()` 中调用两个报告版函数（在正式 12 图之后、Dashboard 之前）。 |
| `write_reports.py` | 1. 新增 `fmt_count()`：计数类指标强制整数格式化；<br>2. 将两处 `fnum(mv(...))` 显示调用改为 `fmt_count()`（`data.bronze_rows`、`data.silver_models_rows`）；<br>3. 将 `executor_run_ms_median` 的 `num_opt(..., 1)` 改为 `num_opt(..., 0)`，消除 `22,801.0` 这类整数时间显示。 |
| `docs/final_refinement_report.md` | 重写 §3「自动测试结果」表格，增加「本轮执行状态」列，区分历史通过记录与本轮实际执行状态，并增加结论说明。 |

由上述生成器重新产出的文件：

- `figures_report/fig07a_global_lineage.{png,pdf,svg}`
- `figures_report/fig07b_representative_family.{png,pdf,svg}`
- `figures_final/*`（未覆盖，原图保留）
- `dashboard_final.html` / `dashboard_standalone.html`
- `data_hubstats/gold/analysis/figure_index.json`
- `report_final.md`
- `docs/rq_analysis_report.md`
- `docs/graph_analysis_report.md`
- `docs/ml_experiment_report.md`
- `docs/benchmark_report.md`
- `docs/latex_report_materials.md`
- `docs/ppt_handoff.md`
- `docs/results_manifest.md`

---

## 3. Figure 7a/7b 数据来源与筛选方法

### 3.1 Figure 7a：全量谱系统计

四项指标全部来自 `results_manifest.json`：

| 显示值 | manifest key | 含义 |
| --- | --- | --- |
| 919,839 | `rq3.n_edges` | Silver 边表 `model_base_edges` 全量计数 |
| 931,066 | `rq3.n_nodes_involved` | (src ∪ dst) 去重节点数 |
| 417,584 | `rq3_gf.viz_subgraph.component_size` | GraphFrames 最大弱连通分量 |
| 43,419 | `rq3.max_children_count` | 单个底座最大直接衍生数 |

布局：2×2 卡片，图幅 6.2 in × 4.6 in，适合 A4 单栏宽度（约 15–16 cm）。

### 3.2 Figure 7b：代表性模型家族

- **底座模型**：`meta-llama/Llama-3.1-8B-Instruct`。选择理由：在现有正式 Figure 7 的两个候选底座中，该底座 relation 构成相对均衡、子节点标签较短，单栏空间下可读性更好。
- **边来源**：`data_hubstats/silver/model_base_edges` Parquet。
- **筛选规则**：按 `relation` 分组，组内按 `downloadsAllTime` 降序；并列时按 `dst` 字典序升序决胜；每类 relation 取前 5 个；不足 5 个时全部保留。总计约 15–20 个节点。
- **边方向**：只绘制 `src=底座 → dst=衍生` 的真实边，不补任何可达性边。
- **颜色与图例**：沿用正式 Figure 7 的 `RELATION_COLORS` / `RELATION_CN`。
- **标注**：仅标注每类 relation 中 downloadsAllTime 最高的 1 个节点，节点标签经 `label_names(width=16)` 截断；中心底座名称完整显示在节点下方白色文本框内。
- **口径声明**：图标题、副标题及脚注均写明「代表性局部子图，非完整继承关系」，不把局部子图规模写成完整家族规模。

---

## 4. A4 单栏缩放检查结果

| 检查项 | 结果 | 说明 |
| --- | --- | --- |
| 图幅 | ✅ | 报告版图宽 6.2 in（≈15.7 cm），符合 A4 单栏 15–16 cm 要求 |
| 矢量格式 | ✅ | 同时提供 PDF/SVG，LaTeX 可直接插入 |
| PNG 分辨率 | ✅ | 300 dpi |
| 中文字体 | ✅ | 沿用 SimHei / Microsoft YaHei 配置 |
| Figure 7a 数字可读性 | ✅ | 2×2 卡片，数字字号 15 pt，A4 单栏缩小后仍可辨认 |
| Figure 7b 底座名称 | ✅ | 中心节点下方完整显示 `meta-llama/Llama-3.1-8B-Instruct` |
| Figure 7b 节点标签 | ✅ | 每类 relation 仅标 1 个，共 4 个标签，分布 4 个象限，无重叠 |
| 图例 | ✅ | 底部横向图例，ncol=3，字号 7.5 pt，可读 |
| 与原图一致性 | ✅ | 颜色、字体、图例与正式 Figure 7 一致；原 `figures_final/fig07_family_network.*` 完整保留 |
| 人工判断 | ✅ | 已实际预览 PNG；节点、标签、底座名称、图例在单栏宽度下均清晰 |

---

## 5. 测试记录如何区分历史结果与本轮结果

在 `docs/final_refinement_report.md` 中新增「本轮执行状态」列：

| 测试脚本 | 本轮执行状态 | 检查项数 | 通过 | 失败 | 依据 |
| --- | --- | --- | --- | --- | --- |
| `test_algorithms.py` | 本轮已执行 | 32 | 32 | 0 | 本轮日志 |
| `test_spark_contracts.py` | **本轮未重新执行**，复用历史结果 | 56 | 56（历史） | 0（历史） | Phase 5 成功日志 / `contract_test_result.json` |
| `check_consistency.py` | 本轮已执行 | 114 | 114 | 0 | 本轮日志 |
| `check_figure_quality.py` | 本轮已执行 | 12 张图 | 0 重叠 / 0 越界 | 0 | 本轮日志 |

结论表述：

> 当前正式项目的历史完整验收记录包含 Spark 契约测试 56 项通过。本轮轻量修复主要重新验证生成器、图表及文档一致性；由于系统资源限制，未重新执行完整 Spark 契约测试。因此历史测试通过记录与本轮实际验证范围分别列示。

---

## 6. 修复的整数格式化问题

### 6.1 已发现的问题

| 位置 | 修复前 | 修复后 |
| --- | --- | --- |
| `report_final.md` §1.3 | `正式分析使用全量 3138954.0 行。` | `正式分析使用全量 3,138,954 行。` |
| `docs/benchmark_report.md` §2 | `CPU/run 时间(ms) 22,801.0` 等 | `CPU/run 时间(ms) 22,801` 等 |
| `docs/benchmark_report.md` §2 | `实测 3,138,959.0 行下` | `实测 3,138,959 行下` |

### 6.2 修复方法

- 新增 `fmt_count(v)`：对计数类指标强制 `int(float(v))` 后千位分隔显示；缺失时返回「（缺失）」。
- 将 `write_reports.py` 中两处显示调用从 `fnum(mv(...))`（返回 float）改为 `fmt_count(mv(...))`。
- 将 `executor_run_ms_median` 的格式化精度从 `nd=1` 改为 `nd=0`，消除整数毫秒时间后的 `.0`。

### 6.3 全局检查结果

重新生成所有 Markdown 报告后，全仓库 Markdown 中不再出现大于等于 4 位数的 `.0` 计数（`grep -R '\b\d{4,}\.0\b'` 无命中）。Gini、RMSE、R²、百分比、运行时间等小数指标保持原有精度，未被错误转换。

---

## 7. 实际运行的测试和结果

> **轮次说明**：不同轮次的测试结果分别记录，未混为一谈。
> - **原始 Phase 5**：`test_algorithms.py` 32 项 + `test_spark_contracts.py` 56 项 + `check_consistency.py` 71 项 = **159 项，失败 0 项**。
> - **最终修复轮（Task E）**：将 `check_consistency.py` 扩展为 114 项（新增 43 项），新增 `check_figure_quality.py` 12 张图；与 Phase 5 合计 **214 项，失败 0 项**。
> - **本轮轻量修复**：只运行轻量检查，**未启动 Spark**，未重新执行 `test_spark_contracts.py`。

| 检查 | 命令 | 结果 |
| --- | --- | --- |
| Python 语法检查 | `python -m py_compile make_figures.py write_reports.py analysis_evo.py check_consistency.py check_figure_quality.py` | 全部通过 |
| Figure 生成测试 | `python make_figures.py` | 正式 12 图 + 旧版补充图 + 报告版 7a/7b 全部生成，退出码 0 |
| 图表文件完整性 | `ls figures_final/ figures_report/` | `figures_final/` 36 个文件；`figures_report/` 6 个文件 |
| 算法单元测试 | `python test_algorithms.py` | 32 项全部通过 |
| 一致性检查 | `python check_consistency.py` | 114 项检查，失败 0 项 |
| 图表质量检查 | `python check_figure_quality.py` | 12 张图：0 重叠、0 越界 |
| 报告与清单一致性 | `python write_reports.py` + 人工抽查 | 关键计数指标无 `.0`，与 `results_manifest.json` 一致 |
| A4 单栏缩放人工检查 | 直接预览 `figures_report/fig07a*.png` 与 `fig07b*.png` | 数字、标签、底座名称、图例均清晰可读 |

---

## 8. 交付物清单

新增：

- `figures_report/fig07a_global_lineage.{png,pdf,svg}`
- `figures_report/fig07b_representative_family.{png,pdf,svg}`
- `docs/final_lightweight_fix_report.md`（本文件）

修改：

- `make_figures.py`
- `write_reports.py`
- `docs/final_refinement_report.md`

重新生成但未新增：

- `figures_final/*`（原 12 图保留，未被覆盖）
- `dashboard_final.html` / `dashboard_standalone.html`
- `report_final.md` 及 `docs/*.md` 报告文件

---

## 9. 尚未解决的问题

本轮未发现新的未解决问题。以下事项按原计划仍需人工完成：

1. `docs/manual_review_checklist.md` 中 7 项语义复核项仍需作者填写「复核结论」。
2. LaTeX 实验报告工程（Trae 维护）仍需后续将 RQ1–RQ5 数值与 Figure 7a/7b 报告版插图填入；**本项目未修改 LaTeX 文件**。

---

**本轮轻量修复已完成，可进入 LaTeX 报告插图阶段。**
