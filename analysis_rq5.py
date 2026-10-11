#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis_rq5.py — RQ5：不同 AI 任务是否表现出不同的生态结构？

分组维度：`pipeline_tag`（缺失统一记 "unknown"）

⚠️ 口径纪律
  1. **只在已标注任务之间做比较**。pipeline_tag 缺失率约 65.6%，missing 被单独
     记为 "unknown" 并单独报告，绝不推断成任何具体任务，也不混进任务排名里比较。
  2. 两种下载口径 `downloads`（近 30 天）与 `downloadsAllTime`（历史累计）
     **各自独立成列**，不混合、不相加。
  3. Gini / Top-k / 分位数全部来自**频数表**，由 `stats_utils` 精确计算
     （频数加权秩公式 + numpy 一致的线性插值分位数），不使用 percentile_approx 近似，
     因此结果与并行度无关、可复现。
  4. **最小样本阈值**：n_models < MIN_GROUP_FOR_COMPARISON 的任务组，
     其 Gini 由极少数模型决定，方差极大，**不可**与大任务组直接比较；
     这类组仍输出（保证完整），但 `comparable = False`。
     注意 `comparable` 的含义是「**有资格参与跨任务比较**」：unknown 组
     （任务标签缺失的占位组）无论样本多大都恒为 `comparable = False`，
     这样 `n_tasks_comparable` 与 local 路径的计数口径完全一致。
  5. `family_size`（= children_count + 1）只是**1 跳**直系家族规模，
     不是真正的家族规模。本脚本同时给出 `avg_children_count`（直接子模型数）
     与 `avg_direct_family_size`，并明确二者关系，避免与连通分量规模混淆。

输出（gold/analysis/）
    rq5_task_ecosystem.csv / rq5_summary.json

用法：python analysis_rq5.py
"""
import json
import os
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS
import stats_utils as SU

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
GOLD = DIRS["gold"]
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(GOLD, "analysis")
os.makedirs(ANALYSIS, exist_ok=True)

# 公开、可复现的最小样本阈值：低于此值的任务组不参与跨任务比较
MIN_GROUP_FOR_COMPARISON = 200
# Gini 还要求取值有足够离散度，否则大量并列值会让 Gini 失去意义
MIN_DISTINCT_VALUES_FOR_GINI = 20

CSV_COLUMNS = [
    "task", "is_unknown", "comparable", "n_models", "n_models_share",
    "downloads_total", "downloadsAllTime_total",
    "downloads_median", "downloads_mean", "downloads_p90", "downloads_p99",
    "downloadsAllTime_median",
    "gini_downloads", "gini_downloadsAllTime",
    "top_1pct_share_downloads", "top_5pct_share_downloads", "top_10pct_share_downloads",
    "top_1pct_share_downloadsAllTime", "top_5pct_share_downloadsAllTime",
    "top_10pct_share_downloadsAllTime",
    "zero_share_downloads", "zero_share_downloadsAllTime",
    "avg_model_age_days", "median_model_age_days",
    "base_relation_coverage", "avg_children_count", "max_children_count",
    "n_models_with_children", "share_models_with_children",
    "avg_direct_family_size", "avg_maintenance_span_days",
]


def group_freq(spark, df, group_col, value_col):
    """按 (group, value) 聚合出频数表，并收集为 dict[group] = (values_asc, freqs)。"""
    freq = (df.groupBy(group_col, value_col)
            .agg(F.count("*").cast("long").alias("freq")))
    n_rows = freq.count()
    print(f"[rq5] 频数表 ({group_col}, {value_col}) rows={n_rows:,}")
    rows = freq.orderBy(group_col, value_col).collect()
    out = {}
    for r in rows:
        key = r[group_col]
        out.setdefault(key, ([], []))
        out[key][0].append(int(r[value_col]))
        out[key][1].append(int(r["freq"]))
    return out


def top_by(rows, key, k=10):
    """按 key 降序取前 k 条（None 视为 0），返回精简列表。"""
    return [{"task": r["task"], "value": r[key]}
            for r in sorted(rows, key=lambda r: -(r[key] or 0))[:k]]


def main():
    spark = SS.build("HF_Analysis_RQ5")
    enriched = spark.read.parquet(os.path.join(GOLD, "models_enriched"))
    n_total = enriched.count()

    # 显式父模型关系覆盖率：来自完整边表（src=父, dst=子）
    edges = spark.read.parquet(os.path.join(SILVER, "model_base_edges")).select("src", "dst")
    parents = (edges.groupBy("dst")
               .agg(F.countDistinct("src").cast("long").alias("parents_count")))

    base = (enriched
            .withColumn("task", F.coalesce(F.col("pipeline_tag"), F.lit("unknown")))
            .withColumn("downloads",
                        F.coalesce(F.col("downloads"), F.lit(0)).cast("long"))
            .withColumn("downloadsAllTime",
                        F.coalesce(F.col("downloadsAllTime"), F.lit(0)).cast("long"))
            .withColumn("model_age_days",
                        F.coalesce(F.col("model_age_days"), F.lit(0)).cast("long"))
            .withColumn("maintenance_span_days",
                        F.coalesce(F.col("update_interval_days"), F.lit(0)).cast("long"))
            .select("model_id", "task", "downloads", "downloadsAllTime",
                    "model_age_days", "maintenance_span_days",
                    "children_count", "family_size")
            .join(parents, on=F.col("model_id") == parents["dst"], how="left")
            .withColumn("parents_count", F.coalesce(F.col("parents_count"), F.lit(0)))
            .drop("dst"))

    # ---- Spark 侧标量聚合 ----
    agg = (base.groupBy("task")
           .agg(F.count("*").cast("long").alias("n_models"),
                F.sum("downloads").cast("long").alias("downloads_total"),
                F.sum("downloadsAllTime").cast("long").alias("downloadsAllTime_total"),
                F.avg("downloads").alias("downloads_mean"),
                F.avg("model_age_days").alias("avg_model_age_days"),
                F.avg("children_count").alias("avg_children_count"),
                F.max("children_count").cast("long").alias("max_children_count"),
                F.sum(F.when(F.col("children_count") > 0, 1).otherwise(0)).cast("long")
                 .alias("n_models_with_children"),
                F.sum(F.when(F.col("parents_count") > 0, 1).otherwise(0)).cast("long")
                 .alias("n_models_with_parent"),
                F.avg("maintenance_span_days").alias("avg_maintenance_span_days"))
           .collect())
    scalars = {r["task"]: r.asDict() for r in agg}

    # ---- 频数表 -> stats_utils 精确计算 ----
    f_dl = group_freq(spark, base, "task", "downloads")
    f_at = group_freq(spark, base, "task", "downloadsAllTime")
    f_age = group_freq(spark, base, "task", "model_age_days")

    rows = []
    for task, s in scalars.items():
        v_dl, c_dl = f_dl.get(task, ([], []))
        v_at, c_at = f_at.get(task, ([], []))
        v_age, c_age = f_age.get(task, ([], []))
        st_dl = SU.concentration_stats(v_dl, c_dl)
        st_at = SU.concentration_stats(v_at, c_at)
        n = int(s["n_models"])
        row = {
            "task": task,
            "is_unknown": task == "unknown",
            # comparable 的语义是「**有资格参与跨任务比较**」，而不只是「样本量够大」。
            # unknown 是 pipeline_tag 缺失的占位组（不是真实任务），按 §口径纪律 1
            # 必须单独报告、不参与任务排名比较，因此这里强制为 False。
            # 若只按样本量判定，unknown（205.8 万）会被标成 True，与 unknown_note
            # 自相矛盾；并且 local 路径按全表求和会得到 n_tasks_comparable=51，
            # 与 Spark 路径（先剔除 labeled 再计数）的 50 不一致。
            "comparable": bool(task != "unknown"
                               and n >= MIN_GROUP_FOR_COMPARISON
                               and st_dl["distinct_values"] >= MIN_DISTINCT_VALUES_FOR_GINI),
            "n_models": n,
            "n_models_share": round(n / n_total, 8),
            "downloads_total": int(s["downloads_total"] or 0),
            "downloadsAllTime_total": int(s["downloadsAllTime_total"] or 0),
            "downloads_median": SU.quantile_from_freq(v_dl, c_dl, 0.5),
            "downloads_mean": round(float(s["downloads_mean"] or 0.0), 4),
            "downloads_p90": SU.quantile_from_freq(v_dl, c_dl, 0.90),
            "downloads_p99": SU.quantile_from_freq(v_dl, c_dl, 0.99),
            "downloadsAllTime_median": SU.quantile_from_freq(v_at, c_at, 0.5),
            "gini_downloads": st_dl["gini"],
            "gini_downloadsAllTime": st_at["gini"],
            "top_1pct_share_downloads": st_dl["top_1pct_share"],
            "top_5pct_share_downloads": st_dl["top_5pct_share"],
            "top_10pct_share_downloads": st_dl["top_10pct_share"],
            "top_1pct_share_downloadsAllTime": st_at["top_1pct_share"],
            "top_5pct_share_downloadsAllTime": st_at["top_5pct_share"],
            "top_10pct_share_downloadsAllTime": st_at["top_10pct_share"],
            "zero_share_downloads": st_dl["zero_value_share"],
            "zero_share_downloadsAllTime": st_at["zero_value_share"],
            "avg_model_age_days": round(float(s["avg_model_age_days"] or 0.0), 4),
            "median_model_age_days": SU.quantile_from_freq(v_age, c_age, 0.5),
            "base_relation_coverage": round(int(s["n_models_with_parent"]) / n, 8),
            "avg_children_count": round(float(s["avg_children_count"] or 0.0), 4),
            "max_children_count": int(s["max_children_count"] or 0),
            "n_models_with_children": int(s["n_models_with_children"] or 0),
            "share_models_with_children": round(
                int(s["n_models_with_children"]) / n, 8),
            "avg_direct_family_size": round(
                float(s["avg_children_count"] or 0.0) + 1.0, 4),
            "avg_maintenance_span_days": round(
                float(s["avg_maintenance_span_days"] or 0.0), 4),
        }
        rows.append(row)

    rows.sort(key=lambda r: -r["n_models"])
    df_out = pd.DataFrame(rows)[CSV_COLUMNS]
    df_out.to_csv(os.path.join(ANALYSIS, "rq5_task_ecosystem.csv"), index=False)
    print(f"[rq5] rq5_task_ecosystem.csv rows={len(df_out)}")

    labeled = [r for r in rows if not r["is_unknown"]]
    unknown = [r for r in rows if r["is_unknown"]]
    comparable = [r for r in labeled if r["comparable"]]

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "spark",
        "snapshot_sha": SA.HUBSTATS_COMMIT,
        "n_models_total": int(n_total),
        "n_tasks_labeled": len(labeled),
        "n_tasks_comparable": len(comparable),
        "min_group_for_comparison": MIN_GROUP_FOR_COMPARISON,
        "min_distinct_values_for_gini": MIN_DISTINCT_VALUES_FOR_GINI,
        "threshold_note": (
            f"n_models < {MIN_GROUP_FOR_COMPARISON} 或唯一取值数 < "
            f"{MIN_DISTINCT_VALUES_FOR_GINI} 的任务组 comparable=False，"
            "其 Gini 由极少数模型决定，不可与大任务组直接比较；"
            "unknown 组（任务标签缺失的占位组）恒为 comparable=False，不参与比较。"),
        "unknown_task": (unknown[0] if unknown else None),
        "unknown_note": (
            "pipeline_tag 缺失的模型统一记为 unknown，单独报告；"
            "所有跨任务比较只在已标注任务之间进行，结论只覆盖已标注部分。"),
        "caliber_note": (
            "downloads（近 30 天）与 downloadsAllTime（历史累计）各自独立成列，"
            "不混合、不相加。"),
        "family_note": (
            "family_size = children_count + 1 只是 1 跳直系家族规模；"
            "avg_direct_family_size 与 avg_children_count 同源，"
            "均不等于连通分量规模或可达后代数（后者见 graph_stats.json）。"),
        "statistics_method": (
            "Gini/Top-k/分位数均由 stats_utils 在频数表上精确计算："
            "频数加权秩公式 + 与 numpy.percentile(linear) 一致的线性插值分位数，"
            "与并行度无关。"),
        "top10_by_n_models": top_by(labeled, "n_models"),
        "top10_by_downloads_total": top_by(labeled, "downloads_total"),
        "top10_by_gini_downloads": [
            {"task": r["task"], "value": r["gini_downloads"]}
            for r in sorted(comparable, key=lambda r: -(r["gini_downloads"] or 0))[:10]],
        "highest_base_relation_coverage": [
            {"task": r["task"], "value": r["base_relation_coverage"]}
            for r in sorted(labeled, key=lambda r: -(r["base_relation_coverage"] or 0))[:10]],
        "rows": rows,
    }
    with open(os.path.join(ANALYSIS, "rq5_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(df_out[["task", "comparable", "n_models", "downloads_median",
                  "gini_downloads", "top_10pct_share_downloads",
                  "base_relation_coverage", "avg_children_count"]].head(15)
          .to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
