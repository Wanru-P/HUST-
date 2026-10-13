#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis_family_ext.py — 增量完善 Task A：模型家族结构的**深度分析**（正式结果）。

设计原则（对齐用户要求）：
  * 复用现有 Silver（models / model_base_edges）与 Gold（analysis/node_metrics），
    **不重新下载**原始 314 万条数据、**不覆盖**任何既有结果文件；
  * 血缘是**有向图**，不假定为树；先求强连通分量（SCC）把环折叠，
    再在**凝聚 DAG** 上定义"最长衍生链"（DAG-only 算法不可直接用于原图）；
  * 结论限定在**显式继承网络**范围内（base_models 缺失率 71.74%，已知）。

产出（`data_hubstats/gold/analysis/`）：
    family_ext_stats.json          标量汇总
    family_depth_dist.csv          root_depth（离根层级）分布
    family_height_dist.csv         subtree_height（最长衍生链）分布
    family_scc_summary.json        SCC / 环 / 自环汇总
    family_multi_parent.csv        多父节点统计
    family_base_metrics.csv        全部底座逐节点家族指标 + 结构判定
    family_structure_classes.csv   Top-N 底座的指标与结构标签
    family_task_structure.csv      按任务类别的家族结构差异

用法：python analysis_family_ext.py
"""
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS
import stats_family as SF

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
os.makedirs(ANALYSIS, exist_ok=True)

TOP_BASES = int(os.environ.get("HF_FAMILY_TOP_BASES", "300"))


def read_edges_pairs(edges_dir):
    """用 pyarrow 读 Silver 边表，返回 (src 列表, dst 列表, relation 列表)。

    用 pyarrow 而非 Spark collect：图算法在 Driver 的纯 Python 里跑，
    直接读 parquet 比 collect 更省内存、更快。
    """
    table = pq.read_table(edges_dir, columns=["src", "dst", "relation"])
    return table.column("src").to_pylist(), table.column("dst").to_pylist(), \
        table.column("relation").to_pylist()


def main():
    t0 = time.time()
    spark = SS.build("HF_Family_Ext")

    models = spark.read.parquet(os.path.join(SILVER, "models")) \
        .select("model_id", "downloads", "downloadsAllTime", "pipeline_tag", "created_at")
    edges = spark.read.parquet(os.path.join(SILVER, "model_base_edges"))
    n_models = models.count()

    # ---------- A. 规模与度数（Spark 原生） ----------
    n_edges = edges.count()
    involved = (edges.select(F.col("src").alias("id"))
                .unionByName(edges.select(F.col("dst").alias("id"))).distinct())
    n_nodes = involved.count()
    n_self_loops = edges.filter(F.col("src") == F.col("dst")).count()

    out_deg = edges.groupBy("src").agg(F.count("*").cast("long").alias("children_count"))
    in_deg = edges.groupBy("dst").agg(F.count("*").cast("long").alias("parents_count"))

    # 多父节点：被 >=2 个底座直接继承的衍生模型
    multi_parent = in_deg.filter(F.col("parents_count") >= 2)
    n_multi_parent = multi_parent.count()
    n_derived = edges.select("dst").distinct().count()
    multi_parent_rows = (in_deg.groupBy("parents_count").agg(F.count("*").alias("n_models"))
                         .orderBy("parents_count").toPandas())
    multi_parent_rows.to_csv(os.path.join(ANALYSIS, "family_multi_parent.csv"), index=False)

    rel_counts = {r["relation"]: int(r["n_edges"]) for r in
                  edges.groupBy("relation").agg(F.count("*").alias("n_edges")).collect()}
    n_merge_edges = rel_counts.get("merge", 0)
    print(f"[family] 边={n_edges} 节点={n_nodes} 自环={n_self_loops} "
          f"多父衍生={n_multi_parent}/{n_derived} ({time.time()-t0:.0f}s)")

    # ---------- B. 逐底座家族指标（Spark 聚合） ----------
    # leaf_child_share：直接子节点中自身无子节点的比例
    edges_with_child_deg = (edges
                            .join(out_deg.withColumnRenamed("src", "dst")
                                  .withColumnRenamed("children_count", "child_children"),
                                  on="dst", how="left"))
    leaf_share = (edges_with_child_deg
                  .groupBy("src")
                  .agg((F.sum(F.when(F.col("child_children").isNull(), 1).otherwise(0))
                        / F.count("*")).alias("leaf_child_share")))
    # merge_share：出边中 relation=merge 的比例
    merge_share = (edges.groupBy("src")
                   .agg((F.sum(F.when(F.col("relation") == "merge", 1).otherwise(0))
                         / F.count("*")).alias("merge_share")))
    # has_multiparent：直接子节点里存在 parents_count>=2 的
    mp_children = (edges.join(in_deg.withColumnRenamed("dst", "dst")
                              .withColumnRenamed("parents_count", "child_parents"),
                              on="dst", how="left")
                   .groupBy("src")
                   .agg(F.max(F.when(F.col("child_parents") >= 2, 1).otherwise(0))
                        .alias("has_multiparent")))
    base_metrics = (out_deg
                    .join(leaf_share, on="src", how="left")
                    .join(merge_share, on="src", how="left")
                    .join(mp_children, on="src", how="left")
                    .withColumnRenamed("src", "base_model"))
    base_pdf = base_metrics.toPandas()

    # 复用 Gold node_metrics 的 descendant_count_khop（只读，不重算）
    nm_path = os.path.join(ANALYSIS, "node_metrics")
    nm = pq.read_table(nm_path, columns=["model_id", "descendant_count_khop"]).to_pandas()
    nm = nm.rename(columns={"model_id": "base_model"})
    base_pdf = base_pdf.merge(nm, on="base_model", how="left")

    # ---------- C. 纯 Python 图算法：SCC / 环 / 深度 / 高度 ----------
    srcs, dsts, rels = read_edges_pairs(os.path.join(SILVER, "model_base_edges"))
    index_map, ordered = SF.build_index(srcs + dsts)
    n_idx = len(ordered)
    pairs_idx = [(index_map[a], index_map[b]) for a, b in zip(srcs, dsts)]
    adj = SF.adjacency_from_edges(n_idx, pairs_idx)
    comps = SF.strongly_connected_components(n_idx, adj)
    comp_of = SF.component_of(n_idx, comps)
    n_comp = len(comps)
    cond_edges = SF.condensation_edges(pairs_idx, comp_of, n_comp)
    cadj = SF.condensation_adjacency(n_comp, cond_edges)
    depth_comp = SF.longest_path_depth(n_comp, cadj)
    height_comp = SF.longest_path_height(n_comp, cadj)
    depth_node = [depth_comp[comp_of[i]] for i in range(n_idx)]
    height_node = [height_comp[comp_of[i]] for i in range(n_idx)]
    print(f"[family] SCC 完成：分量={n_comp} 凝聚边={len(cond_edges)} ({time.time()-t0:.0f}s)")

    # 环（真实强连通环 = 分量 size>1；自环单独计）
    scc_sizes = [len(c) for c in comps]
    cyclic_comps = [c for c in comps if len(c) > 1]
    n_nodes_in_cycles = sum(len(c) for c in cyclic_comps)
    scc_summary = {
        "n_nodes": n_idx,
        "n_strongly_connected_components": n_comp,
        "n_scc_size_gt1": len(cyclic_comps),
        "max_scc_size": max(scc_sizes) if scc_sizes else 0,
        "n_nodes_in_cycles": n_nodes_in_cycles,
        "cycle_node_share": round(n_nodes_in_cycles / n_idx, 8) if n_idx else 0,
        "n_self_loop_edges": int(n_self_loops),
        "condensation_edges": len(cond_edges),
        "condensation_is_dag": True,
        "note": ("强连通分量按 Tarjan 迭代实现；size>1 的分量即真实有向环。"
                 "自环(src==dst)是 size=1 的特殊环，单独计数。"
                 "深度/高度均在**凝聚 DAG** 上计算，因此含环图也有严格定义。"),
    }
    with open(os.path.join(ANALYSIS, "family_scc_summary.json"), "w", encoding="utf-8") as f:
        json.dump(scc_summary, f, ensure_ascii=False, indent=2)

    # 深度/高度分布
    depth_counter = Counter(depth_node)
    height_counter = Counter(height_node)
    pd.DataFrame(sorted(depth_counter.items()), columns=["root_depth", "n_nodes"]) \
        .to_csv(os.path.join(ANALYSIS, "family_depth_dist.csv"), index=False)
    pd.DataFrame(sorted(height_counter.items()), columns=["subtree_height", "n_nodes"]) \
        .to_csv(os.path.join(ANALYSIS, "family_height_dist.csv"), index=False)

    # 把 depth/height 映射回底座
    depth_by_id = {ordered[i]: depth_node[i] for i in range(n_idx)}
    height_by_id = {ordered[i]: height_node[i] for i in range(n_idx)}
    base_pdf["root_depth"] = base_pdf["base_model"].map(depth_by_id)
    base_pdf["subtree_height"] = base_pdf["base_model"].map(height_by_id)
    base_pdf["has_multiparent"] = base_pdf["has_multiparent"].fillna(0).astype(int)

    # ---------- D. 结构判定 ----------
    labels = []
    for row in base_pdf.itertuples(index=False):
        lab, _ = SF.classify_structure(
            int(row.children_count), int(row.subtree_height or 0),
            float(row.leaf_child_share or 0.0), float(row.merge_share or 0.0))
        labels.append(lab)
    base_pdf["structure"] = labels

    base_pdf = base_pdf[[
        "base_model", "children_count", "descendant_count_khop",
        "root_depth", "subtree_height", "leaf_child_share", "merge_share",
        "has_multiparent", "structure"]]
    base_pdf.to_csv(os.path.join(ANALYSIS, "family_base_metrics.csv"), index=False)

    top_bases = base_pdf.sort_values("children_count", ascending=False).head(TOP_BASES)
    top_bases.to_csv(os.path.join(ANALYSIS, "family_structure_classes.csv"), index=False)
    struct_counts = base_pdf["structure"].value_counts().to_dict()
    # 分规模档的结构分布：全部底座里绝大多数只有 1 个子节点（平凡家族），
    # 因此同时给出 children>=10 与 >=100 两档，供报告区分"平凡"与"成规模"家族。
    struct_by_tier = {}
    for tier, thr in (("all", 1), ("ge10", 10), ("ge100", 100)):
        sub = base_pdf[base_pdf["children_count"] >= thr]
        struct_by_tier[tier] = {k: int(v) for k, v in
                                sub["structure"].value_counts().to_dict().items()}

    # ---------- E. 任务类别的家族结构差异 ----------
    base_task = (out_deg
                 .join(models.select("model_id", "pipeline_tag"),
                       out_deg["src"] == models["model_id"], how="left")
                 .select(F.col("src").alias("base_model"), "children_count",
                         F.coalesce(F.col("pipeline_tag"), F.lit("unknown")).alias("task")))
    bt_pdf = base_task.toPandas()
    bt_pdf["subtree_height"] = bt_pdf["base_model"].map(height_by_id)
    bt_pdf = bt_pdf.merge(base_pdf[["base_model", "merge_share", "has_multiparent"]],
                          on="base_model", how="left")
    agg = (bt_pdf.groupby("task")
           .agg(n_bases=("base_model", "count"),
                total_children=("children_count", "sum"),
                median_children=("children_count", "median"),
                avg_height=("subtree_height", "mean"),
                merge_share=("merge_share", "mean"),
                multiparent_share=("has_multiparent", "mean"))
           .reset_index()
           .sort_values("total_children", ascending=False))
    agg.to_csv(os.path.join(ANALYSIS, "family_task_structure.csv"), index=False)

    # ---------- F. 汇总落盘 ----------
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "spark_native + python_graph",
        "snapshot_sha": SA.HUBSTATS_COMMIT,
        "n_models_total": int(n_models),
        "n_edges": int(n_edges),
        "n_nodes_involved": int(n_nodes),
        "n_distinct_base_models": int(out_deg.count()),
        "n_distinct_derived_models": int(n_derived),
        "n_multi_parent_models": int(n_multi_parent),
        "multi_parent_share_of_derived": round(n_multi_parent / n_derived, 6),
        "n_merge_edges": int(n_merge_edges),
        "relation_counts": rel_counts,
        "relation_shares": {k: round(v / n_edges, 6) for k, v in rel_counts.items()},
        "max_children_count": int(base_pdf["children_count"].max()),
        "max_subtree_height": int(base_pdf["subtree_height"].max()),
        "max_root_depth": int(max(depth_node)) if depth_node else 0,
        "mean_subtree_height_all_bases": round(float(base_pdf["subtree_height"].mean()), 4),
        "structure_class_counts_all_bases": {k: int(v) for k, v in struct_counts.items()},
        "structure_class_counts_by_tier": struct_by_tier,
        "scc": scc_summary,
        "top_bases_by_children": top_bases.head(10)[
            ["base_model", "children_count", "subtree_height", "structure"]]
            .to_dict(orient="records"),
        "scope_note": ("结论限定在**显式继承网络**内；base_models 缺失率 71.74%，"
                       "无显式底座的模型不在图内。深度/高度为凝聚 DAG 上的最长路径，"
                       "不等同于可达后代数。"),
        "seconds": round(time.time() - t0, 1),
    }
    with open(os.path.join(ANALYSIS, "family_ext_stats.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

    print(json.dumps({k: summary[k] for k in
                      ["n_edges", "n_nodes_involved", "n_multi_parent_models",
                       "max_subtree_height", "structure_class_counts_all_bases",
                       "seconds"]}, ensure_ascii=False, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
