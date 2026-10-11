# 最终交付修复与可视化优化报告

> 生成时间：2026-10-11 19:10 CST  
> 对应轮次：Phase 5 最终修复（Task A–E）  
> 核心原则：**优先修改生成器代码，不允许只手动改自动生成的 Markdown / HTML。**

## 0. 本轮总体结论

- 本次只修复既有交付物与生成代码的质量问题，**未重新采集数据、未重跑全量 Spark ETL、未重训 MLlib、未重跑 GraphFrames 迭代、未重做 107 组 Benchmark**。
- 所有修复都落实到 `make_figures.py`、`write_reports.py`、`analysis_evo.py`、`README.md`、`check_consistency.py` 等生成器或源码；自动产物（图表、报告、Dashboard、清单）经完整流水线重新生成后，与代码保持同步。
- 自动验收：**32 + 56 + 114 + 12 = 214 项检查全部通过**，0 失败。
- 新缺陷 / 风险已全部清零；剩余 7 项语义复核项见 `docs/manual_review_checklist.md`，需人工完成并填写结论。

## 1. 确实存在的缺陷（必须修复）

| 编号 | 问题 | 修改位置 | 修改内容 | 验证结果 | 类型 |
| --- | --- | --- | --- | --- | --- |
| D1 | **Figure 7 过度依赖 1500 节点密集网络**，A4/A4 单栏缩小后无法阅读 | `make_figures.py::fig07_family_network()` 等 | 重设计为左右双面板：Panel A 展示 4 项完整图统计（919,839 边 / 931,066 节点 / 最大 WCC 417,584 节点 / 最大直接衍生 43,419），全部从 `results_manifest.json` 读取；Panel B 选取 `meta-llama/Llama-3.1-8B-Instruct`、`Qwen/Qwen3-4B-Instruct-2507` 为代表性底座，按 relation 各取 downloadsAllTime 前 10 个下游模型，画 hub-and-spoke 局部子图（40 节点 / 底座），并明确标注「代表性局部子图 · 非完整继承关系」 | 12 张图完整性守卫通过；`check_figure_quality.py` 报告 Figure 7 文本重叠 0、越界 0；浏览器验收 Dashboard 中 Figure 7 清晰显示 | 缺陷 |
| D2 | **Figure 1 首月注释被图例遮挡 76%**，无法阅读 | `make_figures.py::fig01_monthly()` | 将 `xytext` 从 `(0.10·len, 0.80·max)` 改为 `(0.02·len, 0.42·max)` | `check_figure_quality.py` 不再报告重叠；浏览器截图显示注释完全可见 | 缺陷 |
| D3 | **报告 / README / 图例中残留「日期地板效应 / 批量迁移写入」等无证据根因断言** | `write_reports.py`、`make_figures.py`、`README.md`、`analysis_evo.py`、`data_hubstats/gold/analysis/summary_rq1.json` | 将「存在日期地板效应」改为「存在时间戳异常聚集（已观测现象）」；删除「这是数据源批量迁移写入的同一时间戳，属数据伪影」的断言；统一改写为「已观测到的现象 / 根因未确认 / 不作因果推断」 | `check_consistency.py` E3 通过，未在现役交付物中检出根因断言；`summary_rq1.json` 经字段级等式校验只改 `date_floor_note` | 缺陷 |
| D4 | **GraphFrames 叙述存在潜在矛盾**：保留风险描述但没有清晰区分「初始风险 / 策略 / 最终状态」 | `write_reports.py::graph_report()`、`final_report()`、`ppt_handoff()` | 新增三段式叙述：「初始风险 → 应对策略 → 最终状态」，并明确「完成 ≠ 没有代价」；若 `gf_steps_summary()` 检出未完成步骤，自动生成「未完成项如实记录」而非硬编码矛盾文字 | `check_consistency.py` E1 通过；现役交付物中无「未完成」自相矛盾描述 | 缺陷 |
| D5 | **ETL 两个计时数字（142.8 s vs 120.6 s）出现在报告中但可能被误读为同一量** | `write_reports.py::etl_timing_note()` 及各报告 | 新增 4 行说明：142.8 s 是 `run_full_etl.py` 端到端墙钟（含解释器启动、Spark 创建、JVM 退出），120.6 s 是 `spark_etl.py` 脚本内阶段计时（t0 在 SparkSession 就绪后），两者差 22.2 s 为会话建立与进程启停开销 | 报告正文与表注均包含口径说明；未修改任一真实数字 | 缺陷 |
| D6 | **下载双口径存在混用风险**（downloads / downloadsAllTime / children_count / component_size 等） | `write_reports.py::latex_materials()`、`final_report()` 等 | 新增 9 行「口径纪律（易混淆项）」表；caliber ratio 由 `caliber_ratio()` 从清单现算，替代硬编码「27 倍」 | `check_consistency.py` E4 通过；双口径合计比与独立列均一致 | 缺陷 |
| D7 | **Figure 7 的统计数字与生成器代码硬编码风险** | `make_figures.py::build_footer()`、`write_reports.py` 两处报告正文 | 修复生成器中 3 处硬编码数字：`build_footer()` 的 Bronze 行数改为 `manifest_value(load_manifest(), "data.bronze_rows")`；bench_report 的「3,138,959 行」和 final_report 的「3,138,954 行」改为从清单读取 | `check_consistency.py` E5 通过；literal_strings 扫描不再命中硬编码头条数字 | 缺陷 |
| D8 | **`results_manifest.json` 与 `figure_index.json` 图注漂移**：manifest 18:13 生成后，Figure 1 图注在 18:20 被更新为「根因未确认」，导致 manifest 与 figure_index 不一致 | 重新按正确顺序执行生成流水线：`make_figures.py` → `build_manifest.py` → `write_reports.py` | 重跑后 `figure_index.json` 图注与 `results_manifest.json` 逐字一致 | `check_consistency.py` E2 通过 | 缺陷 |

## 2. 视觉与一致性优化（非缺陷，是交付质量提升）

| 编号 | 优化项 | 修改位置 | 修改内容 | 验证结果 | 类型 |
| --- | --- | --- | --- | --- | --- |
| O1 | 旧版 Figure 7 保留为补充材料 | `make_figures.py::fig07_legacy_network()` | 将 1500 节点力导向网络图保存到 `figures_supplementary/fig07_global_network_legacy.{png,pdf,svg}`；Figure Index、Dashboard、报告图注中明确标注其为「旧版网络子图（补充材料，非正式图）」 | 三种格式文件存在；Dashboard 未将其混入主图 | 优化 |
| O2 | Dashboard 可独立分享 | `make_figures.py::dashboard()` 重写 | 新增 `dashboard_standalone.html`（4.7 MB，base64 内嵌全部图片）；保留 `dashboard_final.html`（相对路径，本地可运行） | `check_consistency.py` E2 通过；浏览器验收 standalone 版 12/12 图片解码成功、无外链 | 优化 |
| O3 | Dashboard 信息层级与响应式布局 | `make_figures.py::dashboard()` CSS/HTML | 新增 4 组 KPI、锚点导航、按 RQ 分组的图表区、图表标题与注释来自 `figure_index.json`、`.tablewrap` 承载宽表格、移动端基础可读性 | 浏览器验收：390 px / 768 px 下无横向溢出，仅表格自身可滚动；9 个 h2 区段完整 | 优化 |
| O4 | 自动一致性检查扩展 | `check_consistency.py` | 新增 E1–E5：GraphFrames 状态、图表一致性、生成器防回退、数据一致性（不重算）、避免硬编码 + 人工复核清单 | 114 项检查，0 失败 | 优化 |
| O5 | 自动图表质量检查 | `check_figure_quality.py` | 渲染全部 12 张图并检查文本重叠 / 越界 | 12 张图：0 重叠，0 越界 | 优化 |

## 3. 自动测试结果

> **按真实执行轮次区分的历史记录**：
>
> - **Phase 5 最终验收**：`test_algorithms.py` 32 项 + `test_spark_contracts.py` 56 项 + `check_consistency.py` 71 项 = **159 项检查，失败 0 项**。`test_spark_contracts.py` 的 56 项 Spark 契约测试在真实数据上全部通过，并由 `gold/analysis/contract_test_result.json` 落盘。
> - **Phase 5 之后最终修复轮（Task E）**：将 `check_consistency.py` 从 71 项扩展到 114 项（新增 E1–E5 共 43 项），并新增 `check_figure_quality.py` 12 张图检查。该轮次完整执行后共 **32 + 56 + 114 + 12 = 214 项检查，失败 0 项**。
> - **本轮轻量修复**：只验证生成器、图表及文档一致性，未重新执行完整 Spark 契约测试（系统内存压力：qemu VM + Gradle/Kotlin 守护进程不可终止）。下表区分历史通过记录与本轮实际执行状态。

| 测试脚本 | 所属历史轮次 | 历史通过 | 本轮执行状态 | 本轮通过 | 本轮失败 | 依据 |
| --- | --- | --- | --- | --- | --- | --- |
| `test_algorithms.py` | Phase 5 / 最终修复轮 | 32 | 已重新执行 | 32 | 0 | 本轮日志 |
| `test_spark_contracts.py` | **Phase 5** | 56 | **未重新执行**（复用 Phase 5 历史结果） | — | — | Phase 5 成功日志 / `contract_test_result.json` |
| `check_consistency.py` | 最终修复轮 | 114 | 已重新执行 | 114 | 0 | 本轮日志 / `consistency_check_result.json` |
| `check_figure_quality.py` | 最终修复轮 | 12 张图 | 已重新执行 | 12 张图 | 0 | 本轮日志 |

> **结论**：原始 Phase 5 验收记录为 **159 项**（32+56+71）；最终修复轮将其扩展为 **214 项**（新增 43 项一致性检查 + 12 张图质量检查）。本轮轻量修复重新验证了除 Spark 契约测试之外的全部轻量检查；由于系统资源限制，`test_spark_contracts.py` 未重新执行，其 56 项通过记录来自 Phase 5 历史日志。不同轮次的结果未混为一谈。

## 4. 浏览器视觉验收

使用 Chrome DevTools Protocol 对 `dashboard_final.html`（相对路径版）与 `dashboard_standalone.html`（base64 内嵌版）分别验收：

- `images`: 12，`images_loaded`: 12，`broken`: []，两者一致。
- `h_overflow`: false（桌面 1440 px 无横向溢出）。
- 移动端 390 px / 768 px：`scrollWidth === innerWidth`，只有 `<table>` 元素宽度超过视口且被 `.tablewrap` 承载，页面本身不溢出。
- 区域截图确认 Figure 1 注释可读、Figure 7 显示双面板、9 行「口径与限制」表正常。

## 5. 遗留问题 / 需人工完成

- `docs/manual_review_checklist.md` 中 7 项语义复核项需人工填写「复核结论」栏，并在本报告备注。
- 这些语义项（如「初始风险→最终状态」的时间顺序、统计口径是否始终一致）无法通过关键词禁令自动判定，必须由作者人工通读交付物后确认。

## 6. 修改文件清单（生成器 / 源码级）

- `make_figures.py`
- `write_reports.py`
- `analysis_evo.py`
- `check_consistency.py`
- `README.md`
- `docs/report_final_body.md`（标记为 SUPERSEDED 并修正矛盾）
- `data_hubstats/gold/analysis/summary_rq1.json`（仅 `date_floor_note` 字段）

由上述生成器重新产出的文件：

- `figures_final/*`（36 个文件）
- `figures_supplementary/*`（3 个文件）
- `dashboard_final.html`
- `dashboard_standalone.html`
- `data_hubstats/gold/analysis/results_manifest.json`
- `data_hubstats/gold/analysis/figure_index.json`
- `data_hubstats/gold/analysis/consistency_check_result.json`
- `docs/manual_review_checklist.md`
- `docs/rq_analysis_report.md`
- `docs/graph_analysis_report.md`
- `docs/ml_experiment_report.md`
- `docs/benchmark_report.md`
- `docs/latex_report_materials.md`
- `docs/ppt_handoff.md`
- `docs/results_manifest.md`
- `report_final.md`
