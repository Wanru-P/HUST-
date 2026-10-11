# Phase 0 · 后续修改计划（Phase 1–4）

> 前置结论见 `phase0_environment_audit.md` 与 `phase0_code_audit.md`
> 总原则：**不重写项目、不覆盖旧数据与旧结果、保留脚本入口与输出格式、禁止虚构结果、禁止用合成数据冒充真实数据、禁止把 Trending 头部样本当作全平台总体**
> 本阶段（Phase 0）到此停止，等验收后再进入 Phase 1。

---

## 0. 全局约束与目标配置

### 0.1 运行配置（单机 Windows local 模式）
| 项 | 值 |
|----|----|
| Master | `local[16]`（20 核留 4 核） |
| Driver Memory | 起步 `8g`，OOM 时升 `12g` |
| `spark.sql.shuffle.partitions` | 起步 `16`，扫描 16/32/64 |
| `spark.driver.maxResultSize` | `2g` |
| 临时/checkpoint 目录 | `E:\...\spark_tmp`（经环境变量，避免 C 盘与 `/tmp`） |
| GraphFrames | **本阶段不跑全量**；仅在“边集子图”上验证 |
| `PYSPARK_PYTHON` | 指向所选 Python（见 Phase 1 环境搭建） |

### 0.2 环境搭建（Phase 1 前置，一次性）
1. 新建独立环境（避免污染 anaconda base）：`conda create -n hf_eco python=3.11`（或 venv），安装 `pyspark==4.x`、`py4j`、`graphframes-py`、`requests`、`pandas`、`pyarrow`、`networkx`、`scikit-learn`、`matplotlib`、`fsspec`、`aiohttp`。
2. 安装 **Hadoop winutils.exe + hadoop.dll**（对应 Hadoop 3.4.x），设置 `HADOOP_HOME` 并加入 `PATH`。
3. 设置 `JAVA_HOME=E:\Software\jdk-17`、`PYSPARK_PYTHON`、`PYSPARK_DRIVER_PYTHON`。
4. 首次 GraphFrames 拉 JAR 走 Maven（已实测可达）；如需可加阿里云仓库镜像。

### 0.3 数据接入通道
- 官方 `huggingface.co` 不可达 → 统一经 **`hf-mirror.com`**（项目 README 已认可的公开镜像）下载 `cfahlgren1/hub-stats` 的 `models.parquet`。
- 固定快照：revision `main` @ `2c1e1950`，并把 sha 写入产物元数据，保证可复现。
- **不更换数据集、不使用未知镜像**；若下载失败则停止并报告，不自动降级。

### 0.4 目录与备份策略（贯穿全程）
- 旧产物**一律保留**：重跑前将 `data/`、`figures/`、`dashboard.html`、`report.md` 备份为 `*_v1_legacy`（或复制到 `legacy/`）。
- 新数据写入**新目录**：`data/bronze_hubstats/models.parquet`，不与旧 `data/bronze/models.jsonl` 混放。
- 新结果写入新目录或带 `v2` 后缀，旧文件不动。

---

## Phase 1 · 数据接入与 Schema 适配

**目标**：把 `cfahlgren1/hub-stats` 的 `models.parquet`（3,138,959 行）安全接入 Bronze，并让 ETL 产出与旧版**同构**的 Silver/Gold（列名与语义兼容），使下游分析脚本几乎不用改。

### 1.1 修改哪些文件
| 文件 | 改动 |
|------|------|
| **新增** `download_hubstats.py` | 经 `hf-mirror.com` 下载 `models.parquet` 到 `data/bronze_hubstats/`；校验 sha256 与行数；支持断点续传；失败即报错退出 |
| **新增** `bronze_schema.py` | 统一定义 Bronze 的“规范字段”（model_id/author/created_at/last_modified/downloads/downloads_all_time/likes/pipeline_tag/library_name/tags/base_models/gated/source/ingested_at），供两种来源共用 |
| `spark_etl.py` | 读取层改造：按 `HF_BRONZE_FORMAT`（`jsonl`/`parquet`）或自动探测选择来源；新增 `baseModels` 结构体解析；`createdAt/lastModified` 已是 timestamp，跳过 `to_timestamp`；补 `downloadsAllTime`；`gated` 置 NULL |
| `local_etl.py` | 同上逻辑的 pandas 复刻（保持与 Spark 版**逐列一致**） |
| `requirements.txt` | 追加 `fsspec`、`aiohttp`（下载用），注明环境搭建说明 |
| `docs/` | 新增 `phase1_data_contract.md`（Bronze/Silver/Gold 字段契约与新旧映射表） |

### 1.2 不应修改的文件
`analysis_evo.py`、`analysis_graph.py`、`analysis_ml.py`、`analysis_rq5.py`、`local_analysis.py`、`make_figures.py`、`make_bench_figures.py`、`report.md`、`dashboard.html`、`collect_hf.py`、`enrich_lastmodified.py`（本阶段保持原样，Phase 2/4 再动）。
**旧数据**：`data/bronze/models.jsonl`、`data/silver/*`、`data/gold/*`、`figures/*` 一律不动。

### 1.3 关键 Schema 适配规则（新旧映射）
| 规范字段 | 旧 JSONL 来源 | 新 Parquet 来源 | 类型处理 |
|----------|---------------|-----------------|----------|
| model_id | `id` | `id` | string |
| author | `id.split('/')[0]` | `author` | string（新源直接用） |
| created_at | `createdAt`（ISO str） | `createdAt`（timestamp） | 统一为 timestamp（UTC） |
| last_modified | `lastModified`（ISO str） | `lastModified`（timestamp） | 同上 |
| downloads | `downloads` | `downloads` | int64 |
| downloads_all_time | —（NULL） | `downloadsAllTime` | int64 |
| likes | `likes` | `likes` | int64 |
| pipeline_tag | `pipeline_tag` | `pipeline_tag` | string |
| library_name | `library_name` | `library_name` | string |
| tags | `tags` | `tags` | list\<string\> |
| base_models | `cardData.base_model`（str/list） | **`baseModels.models[].id`**（结构体） | 统一为 list\<string\>；`baseModels` 为空则空列表 |
| gated | `gated` | 无 | 置 NULL（保留列以兼容） |
| source | 新增常量 `hf_api` | 常量 `hub_stats` | string |
| ingested_at | `_scraped_at` | 下载时间/snapshot sha | string |

> ⚠️ `baseModels` 是结构体（`{relation, models:[{_id,id}]}`），**不能**套用旧 `cardData.base_model` 的 `get_json_object` 逻辑；需按 struct 取 `models[*].id` 再 explode。同时保留 `relation`（finetune/quantized/…）可作 RQ3 的边类型增强。

### 1.4 采样策略（避免排序偏差）
- **默认全量**（3.14M 行）跑 Spark ETL。
- 若需 pandas 抽样：**不得取 head(N)**；按 `hash(model_id) % K == 0` 做**确定性随机抽样**，或跨行组等距抽样，并记录抽样率。
- 所有产物元数据写入：`source`、`snapshot_sha`、`n_rows`、`sampled`(bool)、`sample_rate`。

### 1.5 需要运行什么测试
1. `download_hubstats.py`：校验下载文件 sha256 == `d86485a0...`（LFS oid）、行数 == 3,138,959。
2. Schema 契约测试：对 Bronze→Silver 断言列名集合、类型、`downloads_all_time` 非全空、`base_models` 为 list\<string\>。
3. 新旧一致性测试：用**同一批** 1000 条（旧 bronze 的 id 集合）分别走新 ETL，比较 `downloads/likes/pipeline_tag` 是否与旧 Silver 一致。
4. 小样本端到端：`--limit 50000` 跑通 ETL，检查行数、空值率、`family_size>=1`。
5. 排序偏差回归：随机抽 10 个行组各取若干行，确认 ETL 输出不含“按 trending 排前 N”的痕迹。

### 1.6 成功验收条件
- [ ] `data/bronze_hubstats/models.parquet` 存在且 sha256/行数校验通过
- [ ] 全量 Spark ETL 在 8GB Driver 下**无 OOM** 跑完，Silver/Gold 行数 ≈ 3.14M（`downloads` 非空率、`base_models` 边数合理）
- [ ] Silver/Gold **列名与旧版完全一致**（新增列只能追加，不能改名/删列）
- [ ] 旧 `data/bronze/models.jsonl` 与旧 Gold 未被子进程覆盖（校验 mtime/大小）
- [ ] `docs/phase1_data_contract.md` 完成新旧字段映射与实测行数

### 1.7 内存 / 兼容性风险
| 风险 | 说明 | 缓解 |
|------|------|------|
| OOM | 3.14M 行 × 69 列，`safetensors` 等嵌套列很大 | 只 select 需要的列；丢弃 `safetensors/siblings/inferenceProviderMapping` |
| winutils 缺失 | 写 parquet 可能报 `HADOOP_HOME not set` | Phase 0 前置安装 |
| `downloadsAllTime` 语义误用 | 与 `downloads` 混用 | 契约中强制两列并存 |
| 排序偏差 | 行组 0 是 trending 块 | 抽样必须随机化 |
| 下载不稳定 | 镜像 302→CDN | 断点续传 + sha 校验 + 失败即停 |

---

## Phase 2 · RQ1–RQ5 分析逻辑修复

**目标**：在**不改变输出格式**的前提下，修正评估口径、Gini 一致性、RQ4 数据泄漏、RQ3 图规模、`update_interval` 语义，并让 Spark 版与 local 版**结果可交叉验证**。

### 2.1 修改哪些文件
| 文件 | 改动 |
|------|------|
| `analysis_ml.py` | **修数据泄漏**：先 `randomSplit([0.8,0.2], seed=42)`，`fit` 用 train、评估用 test；特征集与 local 版统一（`likes_log`、`model_age_days`、`tag_count`、`children_count`、`family_size`、`update_interval_days` + 类别列）；`maxBins` 按实际基数设 |
| `local_analysis.py` | 与 Spark 版特征/划分口径对齐；Gini 统一到同一公式 |
| `analysis_evo.py` | Gini 口径与 `analysis_rq5.py`、`local_analysis.py` 统一；Top-k 份额明确用 `downloads` 还是 `downloads_all_time`（两者都出） |
| `analysis_rq5.py` | 同上；`MIN_GROUP` 阈值在全量下重估（样本量变大可下调/上调） |
| `analysis_graph.py` | 在**边集子图**上跑（`baseModels` 非空的模型 + 其底座），输出连通分量/PageRank/LPA 到 `gold/analysis/*.json`（当前只打印不落盘）；checkpoint 路径改为跨平台 |
| `benchmark.py` | 提升嵌套函数 `time_op`/`sampler` 到模块级；docstring 去 WSL；**新增真实数据档**（`--real` 用真实 Bronze，非合成） |
| `make_figures.py` | 提升嵌套函数 `_fnum`/`_figs_html` 到模块级；图表标题/口径按新数据更新 |
| `collect_hf.py` / `enrich_lastmodified.py` | `--proxy`/`PROXY` 改为参数化（默认走环境变量），保留但不再是主通道 |

### 2.2 不应修改的文件
`spark_etl.py` / `local_etl.py` 的 **输出 Schema**（Phase 1 已定契约，本阶段不得再改列）；`report.md`/`dashboard.html`（Phase 4 统一重生成）；旧数据目录。

### 2.3 需要运行什么测试
1. **Gini 单元校验**：对已知数组 `[0,10]`、`[0,0,1]`、`[1,2,3]` 断言三处实现结果一致且与理论值一致。
2. **RQ4 泄漏检查**：确认评估只在 test 上进行；train/test 无交集。
3. **双引擎交叉验证**：同一输入下 Spark 版 vs local 版的 Gini/Top-k/连通分量数/PageRank 榜首一致（允许浮点误差）。
4. **RQ3 子图正确性**：抽查若干 `baseModels` 记录，验证边方向（base→derived）与 `children_count`。
5. 回归：旧 1000 样本上重跑，结果应与此前记录**在合理误差内**一致（证明改动未破坏逻辑）。

### 2.4 成功验收条件
- [ ] RQ4 报告 train/test 指标（不再用训练集评估），且两引擎口径一致
- [ ] 三处 Gini 在同一数据上结果一致，且零下载占多数时不为负/不越界 [0,1]
- [ ] `analysis_graph.py` 结果落盘为 JSON，`make_figures.py` 可直接读
- [ ] 全部 `*.py` 无嵌套函数定义（约束符合）
- [ ] 每个 RQ 都有 Spark 版与 local 版结果并附交叉验证说明

### 2.5 风险
| 风险 | 缓解 |
|------|------|
| 改口径后与旧报告数字不一致 | 在报告中明确“口径变更说明”，不追求与旧数字一致 |
| RQ3 全量图过大 | 只跑边集子图；全量 GraphFrames 推迟到用户确认 |
| Spark/local 浮点差异 | 统一 `round(…,4)` 与排序 tie-breaker（按 model_id） |

---

## Phase 3 · 正式 Spark 实验与 Benchmark

**目标**：在真实数据上跑正式 ETL + RQ1–RQ5 + §18 扩展性实验，产出可复现的真实结果（非合成）。

### 3.1 修改哪些文件
- `benchmark.py`：真实数据多档（如按行组分档 25%/50%/100%），算子仍为 read/explode/groupBy/join/graph-build；补 executor CPU / shuffle 字节（若 `StatusTracker` 可取，否则注明“本地模式不可得”）
- **新增** `run_all.py`（编排：下载→ETL→分析→benchmark→出图），统一日志与产物元数据
- `docs/`：`phase3_experiment_log.md`（每次实验的配置、耗时、产物路径、sha）

### 3.2 不应修改的文件
Phase 1/2 已冻结的 Schema 与算法口径；旧结果目录。

### 3.3 需要运行什么测试
1. 冒烟：`local[4]`、8GB、5 万行跑通全链路。
2. 正式：`local[16]`、8→12GB、全量 3.14M 跑 ETL + RQ1/2/4/5。
3. Benchmark：真实数据 3 档规模 × 3 档分区（16/32/64）× 5 算子，重复 3 次取中位数（去 JVM 冷启动）。
4. 复现：同配置连跑两次，关键指标（行数、Gini、RMSE）稳定。

### 3.4 成功验收条件
- [ ] 全量 ETL + 分析在 8GB（必要时 12GB）下完成，无 OOM，产物行数/字段与契约一致
- [ ] `benchmark_scale.csv` 含**真实数据**档位，指标列（时间/CPU/shuffle）有值或明确标注不可得
- [ ] 实验日志记录：配置、耗时、内存峰值、产物 sha
- [ ] 所有结论可回溯到原始数据（无手工数字）

### 3.5 风险
| 风险 | 缓解 |
|------|------|
| 12GB 仍 OOM | 只 select 必要列；调大分区；必要时分阶段落盘 |
| GraphFrames JAR 首次拉取失败 | 预先缓存 ivy 目录；失败即报错不降级 |
| 可用内存不足（审计时仅 6.76GB 空闲） | 跑批前清理后台程序；分阶段执行 |
| 真实数据规模超预期 | 先 50% 档，再 100% |

---

## Phase 4 · Figures、Dashboard、Report 重新生成

**目标**：用正式结果重出 12 张图、Dashboard 与中文报告，并更新 README/环境说明。

### 4.1 修改哪些文件
- `make_figures.py` / `make_bench_figures.py`：读新结果；标题去掉“Demo·1000”，标注真实规模与快照 sha
- `dashboard.html`（由脚本重生成）：KPI 反映全量
- `report.md`：全部数字替换为真实结果；§8 扩展性改为真实数据；新增“口径与局限”章节
- `README.md`：环境改为本机 Windows 说明；采集章节改为 hub-stats 快照流程
- `docs/`：`phase4_deliverables.md`（产物清单与图注）

### 4.2 不应修改的文件
`data/` 下旧结果（保留）；Phase 1–3 冻结的脚本逻辑。

### 4.3 需要运行什么测试
1. 图表生成无异常，12 张 PNG 齐全且非空。
2. Dashboard 内嵌图与表格数字与 `gold/analysis/*` 一致。
3. 报告数字与结果文件逐一核对（抽查 ≥10 处）。
4. 中文标题无“方框”乱码（字体已设 Microsoft YaHei）。

### 4.4 成功验收条件
- [ ] 12 图 + Dashboard + Report 全部基于**真实全量**结果
- [ ] 报告明确：数据源、快照 sha、样本口径、指标定义（尤其 downloads vs downloadsAllTime）
- [ ] 无残留“1000/Demo”表述；无合成数据结论混入
- [ ] 旧交付物保留在 `legacy/`，新交付物路径与旧一致（入口/格式不变）

### 4.5 风险
- 报告结论与旧版差异大 → 属预期（旧版是 trending 样本），需在报告中说明“为何变化”。
- 字体/编码问题 → 固定字体与 UTF-8。

---

## 附：Phase 1 具体执行方案（速览，供验收）

1. **环境**：建 `hf_eco` 环境装 pyspark 4.x / graphframes-py / winutils；设 `JAVA_HOME`、`HADOOP_HOME`、`PYSPARK_PYTHON`。
2. **下载**：`download_hubstats.py` 经 `hf-mirror.com` 取 `models.parquet`（1.49GB，sha `d86485a0…`，3,138,959 行）→ `data/bronze_hubstats/`。
3. **契约**：`bronze_schema.py` 定义规范字段与新旧映射（见 1.3）。
4. **改造 ETL**：`spark_etl.py` / `local_etl.py` 支持读 parquet；解析 `baseModels.models[].id`；补 `downloadsAllTime`；`createdAt/lastModified` 直用 timestamp。
5. **输出**：Silver 三表 + Gold 七表（列名与旧版一致），写入新目录，旧产物不动。
6. **验证**：sha/行数校验 → Schema 契约测试 → 新旧 1000 条一致性 → 5 万行端到端 → 全量 3.14M。
7. **交付**：`docs/phase1_data_contract.md` + 实测行数/空值率报告。

**阻塞问题（需用户决策或前置解决）**
1. **PySpark 未安装**：需确认用 conda 还是 venv，以及是否允许联网装包。
2. **winutils 缺失**：需下载 Hadoop 3.4.x winutils（走 Maven/镜像），需用户许可。
3. **官方 HF 域名不通**：确认统一使用 `hf-mirror.com`（README 已认可）作为下载通道。
4. **可用内存偏低**（审计时 6.76GB）：正式跑批前请清理后台程序。
5. **wsl.exe 被本机安全策略禁用**：确认放弃 WSL、走原生 Windows。
