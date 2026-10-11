# Phase 0 · 代码与数据审计报告

> 项目根：`hf_eco_analysis_交付/hf_eco_analysis/`
> 审计方式：逐文件阅读 + 实测 Schema（pyarrow 读 Parquet footer）
> 原则：只读审计，未修改任何文件

---

## 1. 文件清单与角色

| 文件 | 角色 | 引擎 | 入口产物 |
|------|------|------|----------|
| `collect_hf.py` | 采集 HF `/api/models` → Bronze JSONL | requests | `data/bronze/models.jsonl` |
| `enrich_lastmodified.py` | 逐模型详情接口补 `lastModified`，**原地覆盖 Bronze** | requests | 覆盖 `data/bronze/models.jsonl` |
| `spark_etl.py` | Bronze → Silver(3) → Gold(7+1) | **PySpark** | `data/silver/*`、`data/gold/*` |
| `local_etl.py` | 同上逻辑的 pandas 复刻 | **pandas** | 同上（写 `part-00000.parquet`） |
| `analysis_evo.py` | RQ1 演化 + RQ2 长尾/Gini | **PySpark** | `gold/analysis/{monthly_new_models,pipeline_trend,lorenz,summary.json}` |
| `analysis_graph.py` | RQ3 家族图 | **PySpark + GraphFrames** | `gold/analysis/{top_families,communities}` |
| `analysis_ml.py` | RQ4 相关 + RF/GBT | **PySpark MLlib** | `gold/analysis/{correlations,feature_importance,model_comparison}` |
| `analysis_rq5.py` | RQ5 任务生态 | **PySpark** | `gold/analysis/task_ecosystem` |
| `local_analysis.py` | RQ1–RQ5 等价分析 | **pandas + networkx + sklearn** | `gold/analysis/*`（含 `rq3_detail.json`/`rq4_detail.json`/`family_graph.json`） |
| `benchmark.py` | §18 扩展性实验 | **PySpark** | `gold/analysis/benchmark_scale.csv` |
| `make_figures.py` | Figure 1–9 + dashboard | **matplotlib + networkx** | `figures/fig1–9`、`dashboard.html` |
| `make_bench_figures.py` | Figure 10–12 | **matplotlib** | `figures/fig10–12` |
| `report.md` / `dashboard.html` | 报告与面板 | — | 交付物 |

> ⚠️ 命名提示：`analysis_evo.py`（Spark）与 `local_analysis.py`（pandas）是**两套并行实现**，产物**同名同路径**。当前交付目录里 `gold/analysis/` 下同时存在**扁平 CSV**（local 产物）与 **`part-00000-*.csv` 目录**（Spark 产物），说明两套都跑过、互相覆盖过。

---

## 2. 数据依赖关系图

```
                    ┌──────────────────────────────────────────────┐
   HF API (官方)     │  collect_hf.py ──► data/bronze/models.jsonl   │  ← 网络依赖(当前不可达)
   (当前不可达)       │  enrich_lastmodified.py ──► (覆盖同一 JSONL)   │
                    └──────────────────────────────────────────────┘
                                     │ 读取 JSONL
              ┌──────────────────────┴───────────────────────┐
              ▼                                               ▼
        spark_etl.py                                   local_etl.py
        (PySpark)                                       (pandas)
              │                                               │
              └──────────────► data/silver/*  +  data/gold/* ◄┘
                                     │ (Parquet)
        ┌────────────┬───────────────┼───────────────┬──────────────┐
        ▼            ▼               ▼               ▼              ▼
  analysis_evo  analysis_graph  analysis_ml    analysis_rq5   make_figures
   (Spark)      (Spark+GF)      (Spark MLlib)  (Spark)        (matplotlib)
        │            │               │               │              │
        └────────────┴───────► data/gold/analysis/* ◄┘              │
                                     ▲                              │
                    local_analysis.py (pandas/networkx) ────────────┘
                    （直接从 bronze 重新 build_gold，不读 parquet）
```

关键点：
- **`local_analysis.py` 不读 Silver/Gold Parquet**，它调用 `local_etl.load_bronze()` + `build_gold()` 从 **Bronze JSONL 重算**。
- **`make_figures.py` 读 Gold Parquet**（`models_enriched` 等）+ `gold/analysis/*` 的 CSV/JSON。
- `benchmark.py` 默认**合成数据**（`spark.range`），`--real` 才读 Bronze。

---

## 3. 谁读 JSONL / 谁读 Parquet

| 模块 | 读 Bronze JSONL | 读 Silver/Gold Parquet | 读 analysis CSV/JSON |
|------|:---:|:---:|:---:|
| `spark_etl.py` | ✅ | — | — |
| `local_etl.py` | ✅ | — | — |
| `enrich_lastmodified.py` | ✅（并覆盖） | — | — |
| `benchmark.py` | ✅（仅 `--real`） | — | — |
| `analysis_evo.py` | — | ✅ `gold/models_enriched` | — |
| `analysis_graph.py` | — | ✅ `silver/*` | — |
| `analysis_ml.py` | — | ✅ `gold/model_adoption_features`（回退 `models_enriched`） | — |
| `analysis_rq5.py` | — | ✅ `gold/models_enriched` | — |
| `local_analysis.py` | ✅（经 `local_etl`） | — | — |
| `make_figures.py` | — | ✅ Gold 七表 | ✅ |
| `make_bench_figures.py` | — | — | ✅ `benchmark_scale.csv` |

**含义**：若要接入新数据源，最小改动面是 **ETL 层**（`spark_etl.py` / `local_etl.py` 的 Bronze 读取）；分析层只要 Silver/Gold Schema 不变即可复用。

---

## 4. 谁由 Spark 生成 / 谁由 pandas 生成

- **Spark 路径**：`spark_etl.py`、`analysis_evo.py`、`analysis_graph.py`、`analysis_ml.py`、`analysis_rq5.py`、`benchmark.py`
- **pandas/NetworkX 路径**：`local_etl.py`、`local_analysis.py`、`make_figures.py`、`make_bench_figures.py`
- **当前交付结果的真实来源**：`report.md`/`dashboard.html`/`figures/` 中 RQ1–RQ5 与 9 张图来自 **local 路径**；`benchmark_scale.csv` 及部分 `part-00000-*.csv` 目录来自 **Spark 路径**（`benchmark.py` 合成数据）。
- ⚠️ 报告 §8 的扩展性结论（10w→100w 线性、16 分区甜点区）来自**合成数据**，非真实 HF 数据；需在 Phase 3 用真实数据复测。

---

## 5. Bronze / Silver / Gold 实际 Schema（实测）

### 5.1 Bronze（JSONL，1000 行，第 1 条键值）
| 字段 | 实测类型 | 备注 |
|------|----------|------|
| `_id` | str | Mongo ObjectId |
| `id` | str | 形如 `google/embeddinggemma-2` |
| `cardData` | dict | 内含 `base_model`（字符串或数组） |
| `likes` / `downloads` / `trendingScore` | int | — |
| `private` | bool | — |
| `tags` | list[str] | 含 `base_model:xxx` 前缀标签 |
| `pipeline_tag` / `library_name` | str | — |
| `createdAt` / `lastModified` | **str**（ISO-8601 `...T...Z`） | — |
| `modelId` | str | 与 `id` 重复 |
| `_scraped_at` | str | 采集时间 |
| **无 `author`** | — | Silver 由 `id.split('/')[0]` 派生 |
| **无 `downloadsAllTime`** | — | 故 Silver 中该列恒 NULL |

### 5.2 Silver（Parquet）
`data/silver/models`（**1000 行**）
| 列 | 类型 |
|----|------|
| model_id | large_string |
| author | large_string |
| created_at | large_string（ISO 字符串） |
| last_modified | large_string |
| downloads | int64 |
| **downloadsAllTime** | **double（全 NULL）** |
| likes | int64 |
| pipeline_tag | large_string |
| library_name | large_string |
| tags | list\<string\> |
| **gated** | **null（类型为 null，从未填充）** |
| base_models | list\<string\> |
| scraped_at | large_string |

`data/silver/model_tags`（19249 行）：`model_id`, `tag`
`data/silver/model_base_edges`（**71 行**）：`src`, `dst`, `relation`（报告称 69 边，实际文件 71 行，存在口径不一致）

### 5.3 Gold（Parquet）
`data/gold/models_enriched`（**1000 行**）：在 Silver 基础上追加
`created_ts`(timestamp UTC)、`modified_ts`(timestamp UTC)、`model_age_days`、`update_interval_days`、`downloads_log`、`likes_log`、`tag_count`、`year`、`month_num`、`created_month`、`log_downloads`、`children_count`、`family_size`

Gold 七表：`task_monthly_stats`(248)、`library_monthly_stats`、`tag_monthly_stats`、`download_distribution`(1000)、`model_family_stats`、`model_lifecycle`、`model_adoption_features`(1000)

### 5.4 当前交付结果（供对照，均为 1000 样本）
- RQ2：Gini **0.928**，Top-1% **50.16%**，Top-5% 78.56%，Top-10% 89.54%，零下载 113
- RQ3：节点 1010、边 71、连通分量 **940**、最大分量 18、LPA 社区 944；出度榜首 `Qwen/Qwen3.8-27B`(13)
- RQ4：RF RMSE 2.5892 / R² 0.6484；GBT RMSE 2.5779 / R² 0.6515；重要性 top `likes_log` 0.4261
- RQ1：区间 2022-03 → 2026-10，峰值月 2026-09（314）
> 以上仅为现状记录，**不作为全平台结论**。

---

## 6. 硬编码路径与 Linux 环境依赖（实测）

| 文件:行 | 问题 | 影响 | 建议 |
|---------|------|------|------|
| `collect_hf.py:67` | `--proxy` 默认 `http://127.0.0.1:7897` | Windows/Clash 假设，换机即失效 | 默认改 `None`，用环境变量 |
| `enrich_lastmodified.py:14` | `PROXY = "http://127.0.0.1:7897"` 硬编码 | 同上 | 参数化 |
| `analysis_graph.py:43` | checkpoint 默认 `"/tmp/hf_graphframes_ckpt"` | Linux 路径；Windows 下 `/tmp` 语义异常 | 默认改 `tempfile.gettempdir()` 下子目录 |
| `benchmark.py:22–25` | docstring 硬编码 WSL 路径 `/home/gyin/...`、`/mnt/e/...` | 仅注释误导，不影响运行 | 更新为通用说明 |
| `analysis_ml.py:16`、`analysis_graph.py:7` | 注释引用 `data_sci` 环境 | 注释误导 | 更新 |
| `README.md` §2 / §2.1 | 全篇 WSL `data_sci` + Clash 7897 说明 | 与本机不符 | Phase 4 更新 |

**好消息**：数据路径**没有硬编码**，全部基于 `os.path.dirname(__file__)` + `HF_DATA_DIR` 覆盖，**可移植性良好**。

---

## 7. 工程约束符合性检查

### 7.1 「不允许在函数或循环内部定义函数」——**存在违规**
| 文件:行 | 嵌套函数 | 所属外层 |
|---------|----------|----------|
| `benchmark.py:138` | `def time_op(name, action)` | `run_battery()` |
| `benchmark.py:144` | `def sampler()` | `time_op()`（双层嵌套） |
| `make_figures.py:252` | `def _fnum(f)` | `dashboard()` |
| `make_figures.py:258` | `def _figs_html(names)` | `dashboard()` |

→ Phase 2/3 需把这 4 个函数**提升到模块级**（`sampler` 依赖闭包变量，需改为显式传参）。

### 7.2 「不要为少于五条语句的简单逻辑额外封装函数」
- `make_figures.py` 的 `_read()`/`_load_json()`（各 2–3 行）属边界情况，可保留或内联。

### 7.3 其他
- 未发现虚构实验结果；但**报告 §8 的 benchmark 用合成数据**（已注明），需区分"演示"与"真实结论"。
- `local_etl._write_parquet` 会先删除同目录 `*.parquet` 再写，避免与 Spark 产物混读——设计合理，但**会覆盖 Spark 产物**，接入新数据时需注意。

---

## 8. 必须保留的数据与结果（不可覆盖）

| 路径 | 说明 |
|------|------|
| `data/bronze/models.jsonl` | 原始采集数据（1000 条，含 `_scraped_at`、已补 `lastModified`）——**只增不删** |
| `data/silver/{models,model_tags,model_base_edges}` | 现有清洗表 |
| `data/gold/models_enriched` + 七表 | 现有 Gold |
| `data/gold/analysis/*` | 全部 RQ 结果 + `benchmark_scale.csv` |
| `figures/fig1–fig12` | 现有 12 图 |
| `dashboard.html`、`report.md` | 现有交付物 |
| `docs/phase0_*.md` | 本次交付 |
| 所有 `*.py` 入口 | 保留入口与输出格式 |

**策略**：新数据 → 写入**新的 Bronze 目录**（如 `data/bronze_hubstats/`），旧产物**原地不动**；重跑结果写入新目录或先整体备份为 `*_v1` 后缀。

---

## 9. 新旧数据源对比（关键 Schema 差异）

| 维度 | 旧：HF API JSONL | 新：hub-stats `models.parquet` |
|------|------------------|-------------------------------|
| 行数 | 1,000（**trending 排序的头部样本**） | **3,138,959**（近全平台） |
| 排序 | 按 trendingScore 降序 | 行组 0 = trending 块；**行组 1–62 按 `createdAt` 升序** |
| `createdAt` | 字符串 ISO | **timestamp[us]**（datetime） |
| `lastModified` | 字符串 ISO | **timestamp[us]** |
| `author` | 无（从 id 派生） | **有独立字段** |
| `downloads` | 有（近 30 天） | 有（int64，近 30 天） |
| `downloadsAllTime` | **缺失（恒 NULL）** | **有（int64，累计）** |
| `base_models` | `cardData.base_model`（str 或 list） | **`baseModels` 结构体**：`{relation, models:[{_id,id}]}`，可空 |
| `gated` | 有 | **无此字段** |
| `tags` | list[str] | list[str]（同样含 `base_model:*` 前缀标签） |
| `cardData` | dict | **large_string（JSON 文本）** |
| 额外字段 | — | `trendingScore`、`safetensors`、`transformersInfo`、`siblings`、`gguf`、`inferenceProviderMapping`、`config` |

### 9.1 实测的字段类型（读真实数据，非猜测）
- `id` = `'google/embeddinggemma-2'`（str）
- `author` = `'google'`（str）
- `createdAt` = `datetime(2026,9,14,14,53,42)`（**datetime**）
- `lastModified` = `datetime(2026,10,6,15,29,23)`
- `downloads` = `45605`（int），`downloadsAllTime` = `45605`（int）
- `likes` = `1425`（int）
- `pipeline_tag` = `'feature-extraction'`；`library_name` = `'transformers'`
- `tags` = list[str]
- `baseModels` = `{'relation': 'finetune', 'models': [{'_id': '6a72f2e3...', 'id': 'Qwen/Qwen3.8-27B'}]}`；**可为 `None`**

### 9.2 抽样统计（5 个行组，各 5 万行）
| 行组 | trendingScore | createdAt 区间 | downloads==0 | baseModels 非空 | downloads==downloadsAllTime |
|------|---------------|----------------|--------------|------------------|------------------------------|
| 0 | 0–1362 | 2022-03 ~ 2026-10 | 12241/50000 | 6924/50000 (13.8%) | 14817/50000 |
| 1 | 全 0 | 2022-05 ~ 2022-12 | 20544/50000 | 326/50000 (0.7%) | 19270/50000 |
| 31 | 全 0 | 2025-04 ~ 2025-05 | 25024/50000 | 14085/50000 (28%) | 24353/50000 |
| 61 | 全 0 | 2026-09 | 19401/50000 | 16536/50000 (33%) | 49836/50000 |
| 62 | 全 0 | 2026-09 ~ 2026-10 | 14655/38959 | 12050/38959 (31%) | 38617/38959 |

**推论**
1. **排序陷阱**：行组 0 是 trending 头部（约 5 万行），其余按时间升序。**任何"取前 N 行"都会引入严重偏差**——这正是用户要求避免的"Trending Top-1000 当全平台"问题的升级版。
2. `downloads` 与 `downloadsAllTime` **语义不同**：行组 0 前者合计 2.05e9、后者 5.47e10（差 26 倍）；新模型二者接近。RQ2 集中度分析必须明确用哪一个（建议以 `downloadsAllTime` 为长期注意力主口径，`downloads` 作对照）。
3. 零下载占比极高（24%–50%），长尾极长。
4. `baseModels` 覆盖率随模型新度提升（0.7% → 33%），RQ3 需按年份分层看谱系成熟度。

---

## 10. 分析逻辑层面的待修问题（供 Phase 2 参考）

1. **采样代表性**（最高优先）：旧样本是 trending 头部 1000 条，`report.md` 已声明不可外推，但图表标题仍写"Demo·1000"。新数据需明确"全平台"口径与时间窗。
2. **`downloads` vs `downloadsAllTime` 未区分**：提案 §9 要求两者并存，旧管线只有 `downloads`。
3. **Gini 口径**：`analysis_evo.py` 与 `analysis_rq5.py` 用"升序秩公式" `2S/(n·total)−1`；`local_analysis.py` 用另一升序公式 `(n+1−2Σcum/s)/n`。二者在**含大量 0 值**时数值会不一致，需统一并做小样本交叉验证（如 `[0,10]`、`[0,0,1]`）。
4. **`update_interval_days` 语义**：现用 `lastModified − createdAt`（总更新跨度）当作"更新频率"代理，与提案"update_frequency"含义有偏差，需在报告中明确口径。
5. **RQ3 图规模**：全量 3.14M 节点的 `connectedComponents`/`pageRank` 在单机可能很慢，用户已同意**暂不跑全量 GraphFrames**；建议先限制在"有 baseModels 的边集"子图上跑（边集远小于顶点集）。
6. **RQ4 划分**：Spark 版 `analysis_ml.py` **没有 train/test 划分**（在 `df` 上 `fit` 又 `transform(df)`，等于**在训练集上评估**）；而 local 版做了 80/20。**两套口径不一致，Spark 版存在数据泄漏**，Phase 2 必须修。
7. **RQ4 特征集不一致**：Spark 版用 `likes`(原始值)，local 版用 `likes_log`；Spark 版缺 `model_age` 的对数化处理差异。需统一。
8. **`analysis_graph.py` 未把结果写 JSON**，只在控制台打印 PageRank/连通分量，图表依赖 local 版产物。

---

## 11. 审计结论

- 工程结构清晰、可移植性好，**不需要重写**。
- 最小必要改动集中在三处：**ETL 的 Bronze 读取层**、**RQ4 的评估口径**、**嵌套函数与硬编码路径**。
- 当前所有交付结果均基于 **1000 条 trending 样本**，属于方法验证；正式结论必须用新数据源（3.14M 行）重跑。
