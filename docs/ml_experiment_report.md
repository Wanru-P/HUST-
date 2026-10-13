# RQ4 · 模型属性与下载热度的统计关联（正式结果）

- 生成时间：2026-10-13T12:01:29.387998+00:00
- 数据快照：`cfahlgren1/hub-stats / models.parquet` @ commit `2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`
- 正式样本：快照内全部 3,138,954 个去重模型（源文件物理行 3,138,959，其中 5 个 id 在源数据中重复出现）
- 全部数字来自唯一结果清单 `data_hubstats/gold/analysis/results_manifest.json`，不手抄、不估算


## 0. 研究定位（必须先读）

研究对象是**当前快照**下模型属性与下载热度的统计关联，不是对未来是否会火的预测。label 与 likes 同取自一个时间点，存在同期相关，不能解释为前瞻性预测能力。

因此本节的指标**不是**「预测模型未来热度」的能力，而是「在当前快照下，属性与下载热度之间的统计关联强度」。

## 1. 修复的关键问题：训练/测试分离

原实现在**全量数据**上拟合预处理器与模型，又在**同一份数据**上评估，属于训练集自评，指标虚高。现在的流程是：

1. 先划分 train / test；
2. 所有预处理（one-hot 词表、Top-50 库词表）**只在训练集上 fit**；
3. 模型只在训练集上训练；
4. 指标**只在独立测试集上**计算；
5. 检查主键重复与 train/test 重叠。

| 项 | 值 | 出处 |
| --- | --- | --- |
| 全量行数 | 3,138,954 | `ml_summary.json:n_rows_full` |
| 主键重复组数 | 0 | `ml_summary.json:n_duplicate_model_id_groups` |
| 建模样本量 | 626,269 | `ml_summary.json:sampling` |
| 样本比例 | 19.9515% | `ml_summary.json:sampling` |
| 训练集行数 | 501,049 | `ml_summary.json:split` |
| 测试集行数 | 125,220 | `ml_summary.json:split` |
| train ∩ test | 0 | `ml_summary.json:split`（必须为 0） |
| 随机种子 | 42 | `ml_summary.json:split` |

抽样方式：确定性哈希 crc32(model_id) % 5 == 0。原因：全量 314 万行直接训练 GBT 在本机代价过高；用户明确要求不要盲目对全量做复杂 GBT 训练。

## 2. 特征口径（Spark 与 sklearn 完全一致）

| 类别 | 特征 |
| --- | --- |
| 数值特征 | likes_log, model_age_days, tag_count, children_count, maintenance_span_days |
| 类别特征 | task, library_top |
| 编码方式 | 两类特征均 one-hot；Top-50 库词表只在训练集统计 |
| 目标 | label = log1p(downloads)，downloads 为近 30 天下载计数 |
| 无目标派生输入特征 | True |

⚠️ 命名修正：源列 update_interval_days = lastModified - createdAt，含义是维护跨度（天），不是更新频率；对外统一名 maintenance_span_days。

## 3. 目标与独立测试集上的结果

目标 `y = log(1 + downloads)`；`downloads` 为**近 30 天**下载计数。

| 模型 | RMSE | MAE | R² |
| --- | --- | --- | --- |
| Baseline_mean | 1.7467 | 1.4033 | -0.0000 |
| LinearRegression | 1.3328 | 0.9501 | 0.4177 |
| RandomForest | 1.0139 | 0.5876 | 0.6631 |
| GBT | 1.0606 | 0.6239 | 0.6313 |

`Baseline_mean` 是「用训练集 label 均值做常数预测」的下界参照：如果复杂模型的 R² 没有明显超过基线，就说明这些属性对下载热度的解释力有限。

## 4. 相关分析（描述性）

| 特征 | Pearson | Spearman |
| --- | --- | --- |
| likes_log | 0.3885 | 0.2705 |
| model_age_days | -0.0068 | 0.0045 |
| tag_count | 0.3737 | 0.6530 |
| children_count | 0.0581 | 0.2021 |
| maintenance_span_days | 0.1468 | 0.2296 |

相关系数在建模样本上计算，属**描述性统计**，不参与模型选择。

## 5. 特征重要性

| 特征 | 重要性（RandomForest，one-hot 分列按源列汇总） |
| --- | --- |
| tag_count | 0.412993 |
| library_top | 0.194445 |
| likes_log | 0.151421 |
| task | 0.132633 |
| model_age_days | 0.054842 |
| maintenance_span_days | 0.034599 |
| children_count | 0.019067 |

重要性是 one-hot 分列按源列汇总后的结果，只表示模型内部的统计贡献，不代表因果，也不代表模型质量。若 likes_log 占主导，只能说明‘同一快照内的点赞与下载高度同期相关’。

## 6. 必须写明的限制

- **likes 与 downloads 都取自同一个时间点的快照**，两者存在强烈的同期相关。若特征重要性高度集中在 `likes_log`，只能说明「同一快照内点赞与下载高度同期相关」，**不能**包装成前瞻性预测能力。
- 训练只用了 20% 的确定性哈希样本（约 63 万行），这是为了在本地 32GB 环境下控制 GBT 训练代价；用户明确要求不要为课程作业盲目对 314 万条全量做复杂 GBT 训练。
- 特征重要性表示模型内部的统计贡献，**不代表因果**，也不代表模型质量。
- 模型年龄、维护跨度等特征随快照时间变化，换一个时间点的快照，结论可能不同。

## 7. 对应图

| 图 | 文件 |
| --- | --- |
| Figure 9 | `figures_final/fig09_ml_importance.png` |

