# 数据质量报告（全量 3,138,959 行快照）

> 生成时间：2026-10-11T09:10:50.757130+00:00　｜　快照 commit：`2c1e195090bbcbe1f940cfdcb4ac25b76e48da70`
> 运行配置：`local[6]` / driver 4g / shuffle 64

## 1. 行数与主键

| 项 | 值 |
|---|---|
| Bronze 源文件行数（Parquet 元数据） | 3,138,959 |
| 源内重复 `id` 造成的多余行 | 5 |
| Silver `models` 行数 | 3,138,954 |
| 去重后唯一 `model_id` 数 | 3,138,954 |
| 重复 `model_id` | **0** |
| Gold `models_enriched` 行数 | 3,138,954 |

> **5 行差异说明**：源快照内有 5 个 `id` 各出现 2 次（如 `datalab-to/surya-ocr-2-gguf`），因此 3,138,959 行去重后为 3,138,954 个唯一模型。`model_id` 是主键，去重是正确行为，不是数据丢失。

## 2. 表规模

| 表 | 行数 |
|---|---|
| Silver `models` | 3,138,954 |
| Silver `model_tags` | 20,625,635（唯一 tag 424,435） |
| Silver `model_base_edges` | 919,839（不同父模型 88,333 / 不同子模型 887,032） |
| Gold `task_monthly_stats` | 2,587 |
| Gold `library_monthly_stats` | 7,915 |
| Gold `tag_monthly_stats` | 861,394 |
| Gold `download_distribution` | 3,138,954 |
| Gold `model_family_stats` | 575 |
| Gold `model_lifecycle` | 3,138,954 |
| Gold `model_adoption_features` | 3,138,954 |
| Gold `lorenz_points` | 62,763 |

## 3. 时间与维度

- `created_at` 范围：**2022-03-02T23:29:04.000Z → 2026-10-10T13:13:07.000Z**
- `created_at` 解析失败：**0**
- `created_at` 全部符合 ISO-8601 UTC：**True**
- 不同 `pipeline_tag` 取值：57
- 不同 `library_name` 取值：2,708

**各年份模型数（按快照中可见的创建时间）：**

| 年份 | 模型数 |
|---|---|
| 2022 | 100,392 |
| 2023 | 318,206 |
| 2024 | 740,836 |
| 2025 | 1,103,162 |
| 2026 | 876,358 |

## 4. 下载量集中度（两种口径独立）

| 指标 | `downloads`（近 30 天） | `downloadsAllTime`（历史累计） |
|---|---|---|
| 下载总量 | 2,788,919,780 | 75,949,397,771 |
| Gini | **0.995355** | **0.998117** |
| Top-1% 份额 | 0.971272 | 0.987017 |
| Top-5% 份额 | 0.990904 | 0.996536 |
| Top-10% 份额 | 0.994817 | 0.998409 |
| 零值模型数 | 1,366,793（43.54%） | 1,317,858（41.98%） |
| 唯一下载量取值数 | 14,894 | 47,869 |

> Gini 用**频数加权秩公式**精确计算（`stats_utils.gini_freq_weighted`），不是采样近似。校验脚本用独立路径重算并与 ETL 写出结果对拍，差异 < 1e-9。

## 5. 契约字段缺失率（全量口径）

| 字段 | 缺失率 |
|---|---|
| `model_id` | 0.000000 |
| `author` | 0.000000 |
| `created_at` | 0.000000 |
| `last_modified` | 0.000000 |
| `downloads` | 0.000000 |
| `downloadsAllTime` | 0.000000 |
| `likes` | 0.000000 |
| `pipeline_tag` | 0.655550 |
| `library_name` | 0.509585 |
| `tags` | 0.000000 |
| `gated` | 1.000000 |
| `base_models` | 0.717412 |
| `base_relation` | 0.717412 |

## 6. 聚合守恒校验

| 检查 | 求和 | 等于模型总数 |
|---|---|---|
| 月度新增之和 | 3,138,954 | True |
| `task_monthly_stats.new_models` 之和 | 3,138,954 | True |
| `library_monthly_stats.new_models` 之和 | 3,138,954 | True |

> `tag_monthly_stats` 按 tag 展开，一个模型可贡献多行，因此其之和**大于**模型总数，属预期，不做相等断言。

## 7. 谱系图外部父模型

- 总边数：**919,839**
- 父模型不在本快照内的边（external parent）：**14,838**
（占 1.61%）
- 不同的外部父模型数：**3,620**

**边 relation 分布：**

| relation | 边数 |
|---|---|
| finetune | 354,320 |
| adapter | 329,707 |
| quantized | 187,784 |
| merge | 48,028 |

## 8. 结论

- 硬性断言失败项：**0**
- 总体结论：**通过**
