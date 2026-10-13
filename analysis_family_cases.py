#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis_family_cases.py — 增量完善 Task B：**三个典型家族案例**分析。

案例选择（由真实计算结果驱动，非人工指定）：
  1) 星型：children_count 最大的底座（预期 FLUX.1-dev）；
  2) 链型：children_count>=100 的底座中 subtree_height 最大者（最长衍生链）；
  3) 合并型：children_count>=20 的底座中 merge 出边数最多者。

每个案例整理：
  * 底座与主要衍生模型；直接/多跳规模；
  * 主要衍生关系类型构成；
  * 模型创建时间与衍生时间的关系（子节点相对底座的创建时间差）；
  * 模型自身下载热度 vs 衍生热度（后代下载量聚合）的比较。

结论仅描述**当前快照下的统计关联**，不推断因果机制。

产出（`data_hubstats/gold/analysis/`）：
    family_cases.json            结构化结果
    family_cases_summary.csv     每案例一行标量
    family_cases_children.csv    每案例 Top-20 衍生模型
    family_cases_relations.csv   每案例关系类型构成

用法：python analysis_family_cases.py
"""
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS

DIRS = SA.resolve_dirs()
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
os.makedirs(ANALYSIS, exist_ok=True)
TOP_CHILDREN = 20


def select_cases(metrics_csv):
    """由 family_base_metrics.csv 选出三个案例（星/链/合并）。"""
    d = pd.read_csv(metrics_csv)
    star = d.sort_values("children_count", ascending=False).iloc[0]
    chain_pool = d[d["children_count"] >= 100]
    chain = chain_pool.sort_values("subtree_height", ascending=False).iloc[0]
    d["merge_edges"] = d["merge_share"].fillna(0) * d["children_count"]
    # 合并型必须本身被判为 merge（merge_share>=0.20）且有一定规模，
    # 否则会选到"链型但恰好 merge 边数多"的底座（如 Llama-3-8B-Instruct），失去示例意义。
    merge_pool = d[(d["children_count"] >= 20) & (d["structure"] == "merge")]
    if merge_pool.empty:
        merge_pool = d[d["children_count"] >= 20]
    merge = merge_pool.sort_values("merge_edges", ascending=False).iloc[0]
    return {"star": star, "chain": chain, "merge": merge}


def main():
    t0 = time.time()
    spark = SS.build("HF_Family_Cases")
    edges = spark.read.parquet(os.path.join(SILVER, "model_base_edges"))
    models = spark.read.parquet(os.path.join(SILVER, "models")).select(
        "model_id", "created_at", "downloads", "downloadsAllTime", "pipeline_tag")

    cases = select_cases(os.path.join(ANALYSIS, "family_base_metrics.csv"))
    result = {"generated_at": datetime.now(timezone.utc).isoformat(),
              "snapshot_sha": SA.HUBSTATS_COMMIT,
              "case_selection_rule": {
                  "star": "children_count 最大的底座",
                  "chain": "children_count>=100 中 subtree_height 最大者",
                  "merge": "被判为 merge 且 children_count>=20 中 merge 出边数最多者"},
              "cases": []}
    summary_rows = []
    children_rows = []
    relation_rows = []

    for role, row in cases.items():
        base = row["base_model"]
        base_attr = models.filter(F.col("model_id") == base).first()
        out_edges = edges.filter(F.col("src") == base)
        n_children = out_edges.count()

        # 关系类型构成
        rel = {r["relation"]: int(r["n_edges"]) for r in
               out_edges.groupBy("relation").agg(F.count("*").alias("n_edges")).collect()}
        for k, v in rel.items():
            relation_rows.append({"role": role, "base_model": base,
                                  "relation": k, "n_edges": v})

        # 子节点属性 + 与底座的创建时间差
        child = (out_edges.join(models, out_edges["dst"] == models["model_id"], how="left")
                 .select(F.col("dst").alias("child"), "relation", "created_at",
                         "downloads", "downloadsAllTime", "pipeline_tag"))
        base_created = base_attr["created_at"]
        child = child.withColumn(
            "days_after_base",
            (F.unix_timestamp(F.to_timestamp("created_at"))
             - F.unix_timestamp(F.to_timestamp(F.lit(base_created)))) / 86400.0)
        agg = child.agg(
            F.count("*").alias("n_children"),
            F.sum("downloads").alias("sum_child_downloads"),
            F.sum("downloadsAllTime").alias("sum_child_downloads_all"),
            F.expr("percentile_approx(downloadsAllTime, 0.5)").alias("median_child_dl_all"),
            F.expr("percentile_approx(days_after_base, 0.5)").alias("median_days_after"),
            F.min("days_after_base").alias("min_days_after"),
            F.max("days_after_base").alias("max_days_after"),
            F.sum(F.when(F.col("days_after_base") >= 0, 1).otherwise(0)).alias("n_after_base"),
        ).first()

        top_children = (child.orderBy(F.desc("downloadsAllTime")).limit(TOP_CHILDREN)
                        .toPandas())
        for _, c in top_children.iterrows():
            children_rows.append({"role": role, "base_model": base,
                                  "child": c["child"], "relation": c["relation"],
                                  "created_at": c["created_at"],
                                  "downloads": c["downloads"],
                                  "downloadsAllTime": c["downloadsAllTime"],
                                  "days_after_base": round(float(c["days_after_base"]), 1)
                                  if pd.notna(c["days_after_base"]) else None})

        base_dl = int(base_attr["downloads"] or 0)
        base_dl_all = int(base_attr["downloadsAllTime"] or 0)
        case = {
            "role": role,
            "base_model": base,
            "pipeline_tag": base_attr["pipeline_tag"],
            "created_at": base_created,
            "children_count": int(n_children),
            "descendant_count_khop": int(row["descendant_count_khop"])
            if pd.notna(row["descendant_count_khop"]) else None,
            "subtree_height": int(row["subtree_height"]),
            "structure": row["structure"],
            "relation_counts": rel,
            "child_sum_downloads": int(agg["sum_child_downloads"] or 0),
            "child_sum_downloadsAllTime": int(agg["sum_child_downloads_all"] or 0),
            "child_median_downloadsAllTime": int(agg["median_child_dl_all"] or 0),
            "base_downloads": base_dl,
            "base_downloadsAllTime": base_dl_all,
            "base_vs_children_downloads_ratio": round(base_dl / agg["sum_child_downloads"], 6)
            if agg["sum_child_downloads"] else None,
            "base_vs_children_all_ratio": round(base_dl_all / agg["sum_child_downloads_all"], 6)
            if agg["sum_child_downloads_all"] else None,
            "median_days_after_base": round(float(agg["median_days_after"]), 1)
            if agg["median_days_after"] is not None else None,
            "min_days_after_base": round(float(agg["min_days_after"]), 1)
            if agg["min_days_after"] is not None else None,
            "max_days_after_base": round(float(agg["max_days_after"]), 1)
            if agg["max_days_after"] is not None else None,
            "n_children_after_base": int(agg["n_after_base"] or 0),
            "top_children": top_children[["child", "relation", "downloadsAllTime"]]
            .to_dict(orient="records"),
        }
        result["cases"].append(case)
        summary_rows.append({k: v for k, v in case.items() if k != "top_children"})
        print(f"[cases] {role}: {base}  children={n_children} height={case['subtree_height']} "
              f"({time.time()-t0:.0f}s)")

    # 复用 RQ4 的 children_count 与下载量相关性（只读 ml_summary.json）
    ml_path = os.path.join(ANALYSIS, "ml_summary.json")
    if os.path.exists(ml_path):
        with open(ml_path, encoding="utf-8") as f:
            ml = json.load(f)
        corr = {c["feature"]: {"pearson": c["pearson"], "spearman": c["spearman"]}
                for c in (ml.get("correlations") or [])}
        result["children_count_downloads_correlation"] = corr.get("children_count")
    result["role_note"] = ("星型底座以'被大量浅层复用'为主，链型底座以'被逐层继续衍生'为主，"
                           "合并型底座以'被多父合并'为主；这些是**当前快照的结构差异**，"
                           "不是因果机制，也不代表模型质量。")

    with open(os.path.join(ANALYSIS, "family_cases.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    pd.DataFrame(summary_rows).to_csv(os.path.join(ANALYSIS, "family_cases_summary.csv"),
                                      index=False)
    pd.DataFrame(children_rows).to_csv(os.path.join(ANALYSIS, "family_cases_children.csv"),
                                       index=False)
    pd.DataFrame(relation_rows).to_csv(os.path.join(ANALYSIS, "family_cases_relations.csv"),
                                       index=False)
    print(f"[cases] 完成 {time.time()-t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
