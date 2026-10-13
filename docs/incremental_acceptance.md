# 增量完善 · 验收说明（Task F）

> 生成时间：2026-10-13T12:01:38.349836+00:00  
> 数据快照：`2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`  
> 本文件由 `write_ext_docs.py` 自动生成，**所有数字取自结果文件，未手抄**。

---

## 1. 本轮做了什么

| 任务 | 内容 | 主要产出 |
| --- | --- | --- |
| Task A | 模型家族结构深度分析（有向图 / 环处理 / 多父 / 结构分类 / 任务差异） | `family_ext_stats.json`、`family_*.csv`、`docs/family_structure_analysis.md` |
| Task B | 三个代表性家族案例（星 / 链 / 合并） | `family_cases.json`、`docs/family_cases.md` |
| Task C | 交互式模型家族探索器 | `family_explorer.html`、`docs/family_explorer_guide.md` |
| Task D | 分组集中度与 Theil 组间/组内分解 | `concentration_groups.json`、`docs/concentration_groups.md` |
| Task E | 新增 3 张正式图 | `figures_ext/fig13–15`（PNG/PDF/SVG） |
| Task F | 报告与 PPT 素材更新、文档与验收 | `report_final.md`、`docs/ppt_handoff.md`、本文件 |

**本轮为增量**：不重写工程、不覆盖任何旧数据与旧结果；原 RQ1–RQ5 的结论保持不变，新增内容以**补充**形式并入报告。

---

## 2. 新增文件清单

| 文件 | 说明 |
| --- | --- |
| `analysis_family_ext.py` | Task A 执行脚本 |
| `analysis_family_cases.py` | Task B 执行脚本 |
| `analysis_concentration_groups.py` | Task D 执行脚本 |
| `make_family_explorer.py` | Task C 页面生成脚本 |
| `make_ext_figures.py` | Task E 图表生成脚本 |
| `stats_family.py` | 纯算法层（SCC / 最长路径 / Theil / 结构分类） |
| `test_family_ext.py` | Task A/D 算法单元测试 |
| `write_ext_docs.py` | 本文档等 5 份增量文档的生成器 |
| `vendor/echarts.min.js` | ECharts 5.5.1（离线内嵌） |
| `templates/family_explorer_template.html` | 探索器模板 |
| `figures_ext/fig13–15.{png,pdf,svg}` | 新增图 3 张 × 3 格式 |
| `family_explorer.html` | 离线交互页面 |
| `docs/family_structure_analysis.md` 等 5 份 | 增量文档 |

---

## 3. 运行记录（复现顺序）

```bash
# 0) 算法层单元测试（不需要 Spark）
python test_family_ext.py

# 1) Task A：家族结构（Spark，约 3 分钟）
python analysis_family_ext.py

# 2) Task B：三个案例（Spark）
python analysis_family_cases.py

# 3) Task D：分组集中度（Spark，单次聚合，约 3 分钟）
python analysis_concentration_groups.py

# 4) Task C：交互式探索器（pyarrow，不需要 Spark）
python make_family_explorer.py

# 5) Task E：新增图表（matplotlib）
python make_ext_figures.py

# 6) 重建结果清单 -> 报告 -> 增量文档
python build_manifest.py
python write_reports.py
python write_ext_docs.py

# 7) 回归：算法 + 契约 + 一致性 + 图表完整性
python test_algorithms.py
python test_spark_contracts.py
python check_consistency.py
python make_figures.py   # 含 12 图完整性守卫
```

---

## 4. 指标定义速查

| 指标 | 定义 | 出处 |
| --- | --- | --- |
| `children_count` | 底座出度（直接衍生数） | `family_base_metrics.csv` |
| `descendant_count_khop` | ≤3 跳可达后代 | 同上 |
| `subtree_height` | 凝聚 DAG 上到汇点的最长路径（边数） | 同上 |
| `root_depth` | 凝聚 DAG 上从源点到该点的最长路径 | 同上 |
| `leaf_child_share` | 直接子节点中无出度的比例 | 同上 |
| `merge_share` | 出边中 merge 的比例 | 同上 |
| `structure` | star/chain/merge/mixed（阈值显式） | `stats_family.classify_structure` |
| Gini / Top-k / 零值占比 | 频数加权口径（与全工程一致） | `stats_utils.py` |
| Theil T / 组间占比 | 组间 + 组内分解 | `stats_family.theil_*` |

---

## 5. 图表 ↔ 数据映射

| 图 | 内容 | 数据来源 |
| --- | --- | --- |
| Figure 13 | 三种典型家族结构对比（星 / 链 / 合并） | `family_cases.json` + `model_base_edges` + `family_base_metrics.csv` |
| Figure 14 | 家族深度分布 / 关系类型构成 / 结构分类 | `family_base_metrics.csv` + `family_ext_stats.json` |
| Figure 15 | 底座规模 vs 衍生链深度 / 底座热度 vs 衍生热度 | `family_base_metrics.csv` + `family_cases.json` |

每张图的图注均标注了数据来源与口径；索引见 `figures_ext_index.json`。

---

## 6. 限制（必须与结论一并陈述）

1. 家族结论只覆盖**显式血缘**（底座关系缺失 71.74%）；
2. 结构分类阈值人为设定，换阈值会改变计数；
3. 含环图必须先做 SCC 折叠才能定义「最长链」；
4. 探索器展示的是**有界子图**，不是全量网络；
5. 所有相关/对比都是**同一快照内的统计描述**，不构成因果；
6. 下载量不代表模型质量。

---

## 7. 最终状态

**PROJECT READY FOR REPORT AND PRESENTATION**

全部模块完成，GraphFrames 迭代算法亦在本机跑完，无未完成项。

