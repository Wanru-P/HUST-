# RQ3 · 模型谱系网络分析（正式结果）

- 生成时间：2026-10-13T12:01:29.372466+00:00
- 数据快照：`cfahlgren1/hub-stats / models.parquet` @ commit `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`
- 正式样本：快照内全部 3,138,954 个去重模型（源文件物理行 3,138,959，其中 5 个 id 在源数据中重复出现）
- 全部数字来自唯一结果清单 `data_hubstats/gold/analysis/results_manifest.json`，不手抄、不估算
- 边语义固定为 `src = 底座(base model)` → `dst = 衍生模型(derived)`，方向不可反转

## 0. 方法说明与诚实原则

RQ3 拆成两个脚本，**分工明确**：

| 脚本 | 内容 | 性质 |
| --- | --- | --- |
| `analysis_graph.py` | 出度/入度、直接家族规模、≤K 跳可达后代、relation 分布、外部父模型 | Spark 原生聚合，精确、必定可完成 |
| `analysis_graph_gf.py` | GraphFrames 迭代算法：degrees / connectedComponents / PageRank(正向与反向) / labelPropagation | **初始风险**：迭代算法在大图上开销高；**策略**：独立脚本 + 超时看门狗 + 逐步骤落盘可续跑；**最终状态**见 §6（本次全部完成） |

**NetworkX 仅用于小规模验证与可视化**（`local_analysis.py`），**绝不用 NetworkX 结果冒充 GraphFrames 结果**。

## 1. 图规模（完整边表，未抽样）

| 指标 | 数值 | 出处 |
| --- | --- | --- |
| 血缘边数 | 919,839 | `graph_stats.json:n_edges` |
| 不同底座（父）模型 | 88,333 | `graph_stats.json:n_distinct_base_models` |
| 不同衍生（子）模型 | 887,032 | `graph_stats.json:n_distinct_derived_models` |
| 参与血缘的模型（并集去重） | 931,066 | `graph_stats.json:n_nodes_involved` |
| 既是底座又是衍生的模型 | 44,299 | `graph_stats.json:n_nodes_both_base_and_derived` |
| 朴素相加（会重复计数，仅作对照） | 975,365 | `graph_stats.json:n_base_plus_derived` |
| 无显式底座的模型 | 2,251,922 | `graph_stats.json:n_models_without_base` |

⚠️ 节点数必须取并集：`975,365`（父+子直接相加）会把同时扮演两种角色的 44,299 个模型算两次；真正的节点数是 **931,066**。

## 2. 外部父模型（快照外引用）

- 父模型不在本快照内的边：**14,838** 条（占全部边 1.6131%），涉及 3,620 个不同的外部父模型。
- 这些边**被完整保留**，没有被静默丢弃；在图中它们指向的节点标记为外部父模型（`node_metrics` 中其下载量等属性为空）。
- 影响：跨样本的谱系会被截断在快照边界处，因此「后代总数」类指标是**下界**。

## 2.1 自环边（数据噪声，保留不删）

- `src == dst` 的自环边：**（缺失）** 条，涉及 （缺失） 个模型（模型把自己列为自己的 base model）。
- 本分析**保留**这些边、不做静默删除；但它们会让该节点单独构成一个规模为 1 的弱连通分量，解读 §6 的分量规模分布时需知悉。

## 3. 家族规模指标（四者含义不同，禁止互相替代）

| 指标 | 定义 | 本快照最大值 |
| --- | --- | --- |
| `children_count`（出度） | 被多少模型**直接**当作底座 | 43,419 |
| `direct_family_size` | `children_count + 1`（1 跳直系，含自身） | 43,420 |
| `descendant_count_khop` | ≤3 跳可达后代数（有界） | 43,682 |
| `component_size` | 弱连通分量规模（GraphFrames connectedComponents） | 见 §6 |

平均出度（按参与血缘的节点计）为 10.4133。

## 4. 边关系类型分布

| relation | 边数 |
| --- | --- |
| finetune | 354,320 |
| adapter | 329,707 |
| quantized | 187,784 |
| merge | 48,028 |

## 5. 头部底座模型（Top-10，按直接衍生数）

| 底座模型 | 直接衍生数 | downloads | downloadsAllTime |
| --- | --- | --- | --- |
| black-forest-labs/FLUX.1-dev | 43,419 | 661,619 | 34,112,217 |
| Qwen/Qwen1.5-0.5B | 32,534 | 38,442 | 4,105,872 |
| Qwen/Qwen1.5-1.8B | 30,626 | 12,777 | 3,367,195 |
| google/gemma-2b | 24,036 | 129,505 | 10,250,038 |
| distilbert/distilbert-base-uncased | 13,182 | 7,837,070 | 693,258,213 |
| stabilityai/stable-diffusion-xl-base-1.0 | 10,998 | 4,301,092 | 126,659,657 |
| google/gemma-7b | 9,585 | 14,850 | 3,209,443 |
| Qwen/Qwen3-4B-Instruct-2507 | 8,570 | 3,563,213 | 63,918,418 |
| lerobot/smolvla_base | 8,100 | 123,058 | 787,776 |
| meta-llama/Llama-3.1-8B-Instruct | 7,237 | 6,015,326 | 188,968,200 |

## 6. GraphFrames 迭代算法实际执行情况

- 执行状态：**`completed`**
- 预算：2,400 秒；图规模：顶点 931,066 / 边 919,839
- 图中孤立模型（不参与任何边，各自构成 1 个单点分量）：2,207,888

| 算法 | 耗时（秒） | 状态 |
| --- | --- | --- |
| degrees（度） | 3.9 | 已完成 |
| connectedComponents（弱连通分量） | 352.9 | 已完成 |
| pageRank 正向 base→derived | 375.0 | 已完成 |
| pageRank 反向 derived→base | 348.9 | 已完成 |
| labelPropagation（社区） | 215.0 | 已完成 |
| 合并逐节点指标 | 13.6 | 已完成 |

### 已完成的迭代算法结果

- **弱连通分量**：图内 42,890 个，加上图外孤立模型共 2,250,778 个。
- 最大连通分量规模见 `component_size_dist.csv` / `largest_components.csv`。

### 迭代算法完成状态：无未完成项

全部 6 个迭代步骤均在 32GB 本机、`local[6]` 条件下实际跑完：度数统计 3.9 秒、connectedComponents 352.9 秒、PageRank（正向） 375.0 秒、PageRank（反向） 348.9 秒、labelPropagation 215.0 秒、逐节点指标合并 13.6 秒。
- **初始风险**：迭代类图算法在 93 万节点 / 92 万边规模上开销高，存在超出本机预算的可能；
- **应对策略**：把迭代算法拆到独立脚本（`analysis_graph_gf.py`）、加超时看门狗、每个步骤单独落盘以便续跑，并对同快照已落盘的结果做复用；
- **最终状态**：`status = completed`，上表耗时均为真实测量值，不存在未完成步骤，也**没有**用 NetworkX 结果替代 GraphFrames 结果。
- 之所以保留这段过程记录：**完成 ≠ 没有代价**。各步骤单次仍需数百秒，内存与时间开销是真实约束，不能因为最终跑通就把它描述成「轻松完成」。

### PageRank 方向的解释纪律

- 正向图（base → derived）的 PageRank 高分节点是**衍生链的末端**，**不是**「基础模型影响力」；
- 反向图（derived → base，由 `analysis_graph_gf.py` 独立构建）的高分节点才是「被大量模型当作底座依赖」的中心；
- **PageRank 只反映图结构中的被引用强度，不等于模型质量**，也不代表技术水平。

## 7. 对应图

| 图 | 文件 | 说明 |
| --- | --- | --- |
| Figure 7 | `figures_final/fig07_family_network.png` | 左 Panel A：**全量图**统计（边/节点/最大弱连通分量/最大直接衍生数）；右 Panel B：两个代表基础模型的 1 跳**代表性局部子图**（按 relation 着色） |
| Figure 7（补充） | `figures_supplementary/fig07_global_network_legacy.png` | 旧版最大弱连通分量 Top-1500 力导向网络，**补充材料**（节点过密、缩小后不可读） |
| Figure 8 | `figures_final/fig08_family_size.png` | direct_family_size 分布 vs 连通分量规模分布 |

