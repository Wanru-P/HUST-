#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis_concentration_groups.py — 增量完善 Task D：**分组集中度**分析。

在不改变原有 RQ2 结果的前提下，按三个维度对下载量集中度做补充比较：
  * task         任务类型（pipeline_tag，缺失记 unknown）
  * created_year 创建年份（取自 created_at 前 4 位）
  * has_lineage  是否有显式继承关系（model_id 是否出现在边的 dst 端）

两种口径严格区分：downloads（近 30 天）与 downloadsAllTime（历史累计）。
用 Theil T 指数把总体不均衡分解为**组间 + 组内**贡献。

统计关联一律不做因果解释。

实现要点（性能）：
    每个 (分组维度 × 口径) 只做**一次** Spark 聚合，产出完整的
    (group, value, freq) 频数长表，随后在 pandas 内按组切分计算
    Gini / Top-k / Theil —— 因此 Spark 作业总数固定为 3×2=6 个，
    而不是"每组一次动作"（后者在 40+ 个任务类型上会慢一个数量级）。

产出（`data_hubstats/gold/analysis/`）：
    concentration_groups.json    结构化结果（含 Theil 分解）
    concentration_groups.csv     长表（grouping, group, caliber, 指标）

用法：python analysis_concentration_groups.py
"""
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS
import stats_family as SF
import stats_utils as SU

DIRS = SA.resolve_dirs()
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
os.makedirs(ANALYSIS, exist_ok=True)

CALIBERS = ["downloads", "downloadsAllTime"]
GROUPINGS = ["task", "created_year", "has_lineage"]
BIG_GROUP_MIN = 1000  # 组规模下限：低于此值的组不进汇总对比（避免小样本噪声）


def log(msg):
    print(f"[groups] {msg}", flush=True)


def freq_long_table(df, grouping, caliber):
    """单次聚合得到 (group, value, freq) 频数长表（pandas）。"""
    out = (df.groupBy(grouping, F.col(caliber).alias("value"))
           .agg(F.count("*").alias("freq"))
           .toPandas())
    return out


def main():
    t0 = time.time()
    spark = SS.build("HF_Concentration_Groups")

    models = (spark.read.parquet(os.path.join(SILVER, "models"))
              .select("model_id", "downloads", "downloadsAllTime", "pipeline_tag",
                      F.substring("created_at", 1, 4).alias("created_year")))
    edges = spark.read.parquet(os.path.join(SILVER, "model_base_edges"))
    derived = (edges.select(F.col("dst").alias("model_id")).distinct()
               .withColumn("has_lineage", F.lit("yes")))
    df = (models
          .withColumn("task", F.coalesce(F.col("pipeline_tag"), F.lit("unknown")))
          .join(derived, on="model_id", how="left")
          .withColumn("has_lineage", F.coalesce(F.col("has_lineage"), F.lit("no"))))
    df.cache()
    n_total = df.count()
    log(f"总模型={n_total} ({time.time()-t0:.0f}s)")

    result = {"generated_at": datetime.now(timezone.utc).isoformat(),
              "snapshot_sha": SA.HUBSTATS_COMMIT,
              "n_models_total": int(n_total),
              "calibers": CALIBERS,
              "groupings": {}}
    rows = []

    for grouping in GROUPINGS:
        result["groupings"][grouping] = {}
        for caliber in CALIBERS:
            long_df = freq_long_table(df, grouping, caliber)
            per_group = {}
            group_freqs = {}
            n_groups = 0
            for g, sub in long_df.groupby(grouping, sort=True):
                key = str(g)
                sub = sub.sort_values("value")
                vals = [int(v) for v in sub["value"].tolist()]
                freqs = [int(f) for f in sub["freq"].tolist()]
                st = SU.concentration_stats(vals, freqs)
                st["theil"] = SF.theil_t_from_freq(vals, freqs)
                per_group[key] = st
                group_freqs[key] = (vals, freqs)
                n_groups += 1
                rows.append({"grouping": grouping, "group": key, "caliber": caliber,
                             "n_models": st["n_models"],
                             "distinct_values": st["distinct_values"],
                             "total": st["total_downloads"],
                             "gini": st["gini"],
                             "top_1pct_share": st["top_1pct_share"],
                             "top_10pct_share": st["top_10pct_share"],
                             "zero_value_share": st["zero_value_share"],
                             "theil": st["theil"]})
            decomp = SF.theil_decomposition_from_freq(group_freqs)
            big = {k: v for k, v in per_group.items() if v["n_models"] >= BIG_GROUP_MIN}
            result["groupings"][grouping][caliber] = {
                "n_groups": n_groups,
                "n_groups_ge_1000": len(big),
                "theil_decomposition": decomp,
                "per_group": per_group,
            }
            if decomp["theil_total"]:
                log(f"{grouping}/{caliber}: 组数={n_groups} (>=1000 的 {len(big)}) "
                    f"T_total={decomp['theil_total']:.4f} "
                    f"between_share={decomp['between_share']:.4f} "
                    f"({time.time()-t0:.0f}s)")
            else:
                log(f"{grouping}/{caliber}: 组数={n_groups} 无法分解 Theil "
                    f"({time.time()-t0:.0f}s)")
        log(f"{grouping} 完成 ({time.time()-t0:.0f}s)")

    with open(os.path.join(ANALYSIS, "concentration_groups.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    pd.DataFrame(rows).to_csv(os.path.join(ANALYSIS, "concentration_groups.csv"), index=False)

    log(f"完成 {time.time()-t0:.0f}s -> concentration_groups.json / .csv")
    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
