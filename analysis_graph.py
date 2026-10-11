#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis_graph.py — RQ3：模型谱系网络的 **Spark 原生图统计**（不依赖 GraphFrames）。

为什么要和 GraphFrames 拆开：
    Phase 2 实测（`spark_graph_smoke.py`）表明，GraphX/GraphFrames 在**20 个节点**的图上
    `connectedComponents` 就要 46 s、`pageRank` 就要 55 s —— 几乎全是固定开销。
    全量图有约 97 万节点 / 92 万边，迭代算法能否在本机完成是**未知数**。
    因此本脚本先把**不依赖迭代、精确可算**的图统计全部算出来并落盘，
    保证 RQ3 至少有可信结果；迭代算法交给 `analysis_graph_gf.py` 单独跑，
    跑不完就如实记录，绝不用 NetworkX 结果冒充。

边语义（**必须保留方向**）：`src` = base model（被继承的底座），`dst` = 衍生模型。
因此：
    children_count（出度）  = 一个底座被多少模型直接当 base      —— 精确
    direct_family_size      = children_count + 1                —— 精确
    component_size          = 弱连通分量规模（GraphFrames 算）   —— 见 gf 脚本
    descendant_count_khop   = ≤K 跳可达后代数（本脚本迭代自连接）—— 有界，明确标注 K

输出（`gold/analysis/`）：
    graph_stats.json          全部标量统计
    top_families.csv          Top-20 底座（按出度）
    out_degree_dist.csv       出度分布
    in_degree_dist.csv        入度分布
    relation_counts.csv       边 relation 分布
    external_parents.csv      Top-20 外部父模型（不在快照内）
    descendant_top.csv        Top-20（按 ≤K 跳可达后代数）
    node_metrics/             (model_id, children_count, direct_family_size, descendant_count_khop)

用法：python analysis_graph.py
"""
import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
os.makedirs(ANALYSIS, exist_ok=True)

TOP_N = 20
# ≤K 跳可达后代：K 越大越接近真实"后代总数"，但中间结果规模随 out-degree 幂增长。
REACH_HOPS = int(os.environ.get("HF_REACH_HOPS", "3"))


def collect_csv(df, name, order_by=None):
    """把**小结果**（Top-N / 分布）收集到 Driver 并写成扁平 CSV。

    只有聚合后的少量行会被收集（Top-20 / 度数分布 ≤ 数百行），
    不是把百万级明细拉到 Driver。
    """
    d = df.orderBy(*order_by) if order_by else df
    pdf = d.toPandas()
    path = os.path.join(ANALYSIS, name)
    pdf.to_csv(path, index=False)
    print(f"[rq3] {name:28s} rows={len(pdf)}")
    return pdf


def main():
    t0 = time.time()
    spark = SS.build("HF_Analysis_Graph")
    models = spark.read.parquet(os.path.join(SILVER, "models")) \
        .select("model_id", "downloads", "downloadsAllTime")
    edges = spark.read.parquet(os.path.join(SILVER, "model_base_edges"))
    n_models = models.count()

    # ---- A. 基本规模 ----
    n_edges = edges.count()
    n_base = edges.select("src").distinct().count()
    n_derived = edges.select("dst").distinct().count()
    # 注意：n_base + n_derived **不是**图的节点数。
    # 一个模型可以既是别人的底座、又是别人的衍生（例如中间层的微调模型），
    # 直接相加会重复计数。真正的节点数必须取并集。
    involved = (edges.select(F.col("src").alias("id"))
                .unionByName(edges.select(F.col("dst").alias("id"))).distinct())
    n_nodes = involved.count()

    # ---- A2. 自环：src == dst（模型把自己列为自己的底座）----
    # 这是真实存在的数据噪声，**保留不删除**（静默丢弃会改变节点/边口径），
    # 但必须显式计数并说明：自环会让该节点单独构成一个规模为 1 的弱连通分量。
    self_loops = edges.filter(F.col("src") == F.col("dst"))
    n_self_loops = self_loops.count()
    n_self_loop_nodes = self_loops.select("src").distinct().count()

    print(f"[rq3] 边={n_edges}  不同底座={n_base}  不同衍生={n_derived}  "
          f"节点(去重并集)={n_nodes}  自环={n_self_loops}  ({time.time()-t0:.0f}s)")

    # ---- B. 外部父模型（引用了快照内不存在的 base model）----
    ext = edges.join(models.select(F.col("model_id").alias("src")), on="src", how="left_anti")
    n_ext_edges = ext.count()
    n_ext_parents = ext.select("src").distinct().count()
    print(f"[rq3] 外部父模型边={n_ext_edges}  不同外部父模型={n_ext_parents}")

    # ---- C. 出度 / 入度 ----
    out_deg = edges.groupBy("src").agg(F.count("*").cast("long").alias("children_count"))
    in_deg = edges.groupBy("dst").agg(F.count("*").cast("long").alias("parents_count"))
    out_dist = out_deg.groupBy("children_count").agg(F.count("*").alias("num_models")) \
        .orderBy("children_count")
    in_dist = in_deg.groupBy("parents_count").agg(F.count("*").alias("num_models")) \
        .orderBy("parents_count")

    # Top 底座：附上下载量与名称，便于解释
    top_fam = (out_deg.join(models, out_deg["src"] == models["model_id"], how="left")
               .select(F.col("src").alias("base_model"),
                       "children_count", "downloads", "downloadsAllTime")
               .orderBy(F.desc("children_count"), F.asc("base_model")).limit(TOP_N))
    top_fam_pdf = top_fam.toPandas()
    top_fam_pdf.to_csv(os.path.join(ANALYSIS, "top_families.csv"), index=False)
    print(f"[rq3] top_families.csv rows={len(top_fam_pdf)}")

    collect_csv(out_dist, "out_degree_dist.csv", ["children_count"])
    collect_csv(in_dist, "in_degree_dist.csv", ["parents_count"])
    rel = edges.groupBy("relation").agg(F.count("*").alias("n_edges")) \
        .orderBy(F.desc("n_edges"))
    collect_csv(rel, "relation_counts.csv", ["n_edges"])
    ext_top = (ext.groupBy("src").agg(F.count("*").cast("long").alias("num_derived"))
               .orderBy(F.desc("num_derived"), F.asc("src")).limit(TOP_N))
    ext_top.toPandas().to_csv(os.path.join(ANALYSIS, "external_parents.csv"), index=False)

    # ---- D. ≤K 跳可达后代（迭代自连接，有界且明确标注 K）----
    reach = edges.select(F.col("src").alias("a"), F.col("dst").alias("b")).distinct()
    for _ in range(REACH_HOPS - 1):
        step = (reach.join(edges.select(F.col("src").alias("b"), F.col("dst").alias("c")), on="b")
                .select("a", F.col("c").alias("b")))
        reach = reach.unionByName(step).distinct()
        print(f"[rq3] ≤{REACH_HOPS} 跳可达中间规模 = {reach.count():,}  ({time.time()-t0:.0f}s)")
    desc = reach.groupBy("a").agg(F.countDistinct("b").cast("long").alias("descendant_count_khop"))
    desc_top = (desc.join(models, desc["a"] == models["model_id"], how="left")
                .select(F.col("a").alias("base_model"), "descendant_count_khop",
                        "downloads", "downloadsAllTime")
                .orderBy(F.desc("descendant_count_khop"), F.asc("base_model")).limit(TOP_N))
    desc_top.toPandas().to_csv(os.path.join(ANALYSIS, "descendant_top.csv"), index=False)

    # ---- E. 逐节点指标表 ----
    nodes = (models
             .join(out_deg, models["model_id"] == out_deg["src"], how="left")
             .drop("src")
             .join(desc, models["model_id"] == desc["a"], how="left")
             .drop("a")
             .withColumn("children_count", F.coalesce(F.col("children_count"), F.lit(0)).cast("long"))
             .withColumn("descendant_count_khop",
                         F.coalesce(F.col("descendant_count_khop"), F.lit(0)).cast("long"))
             .withColumn("direct_family_size", F.col("children_count") + 1))
    nodes.write.mode("overwrite").parquet(os.path.join(ANALYSIS, "node_metrics"))

    # ---- F. 汇总 ----
    deg_stats = out_deg.agg(F.max("children_count").alias("max_out"),
                            F.avg("children_count").alias("avg_out")).first()
    desc_stats = desc.agg(F.max("descendant_count_khop").alias("max_desc"),
                          F.avg("descendant_count_khop").alias("avg_desc")).first()
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "spark_native",
        "snapshot_sha": SA.HUBSTATS_COMMIT,
        "n_models_total": int(n_models),
        "n_edges": int(n_edges),
        "n_distinct_base_models": int(n_base),
        "n_distinct_derived_models": int(n_derived),
        # 真正的节点数 = 并集去重（与 graphframes_result.json 的 n_vertices_in_graph 一致）
        "n_nodes_involved": int(n_nodes),
        # 保留朴素求和以便说明差异：n_base + n_derived - n_nodes = 既是底座又是衍生的模型数
        "n_base_plus_derived": int(n_base + n_derived),
        "n_nodes_both_base_and_derived": int(n_base + n_derived - n_nodes),
        "node_count_note": (
            "n_nodes_involved 是 (src ∪ dst) 去重后的节点数，"
            "与 graphframes_result.json 的 n_vertices_in_graph 应完全相等；"
            "n_base_plus_derived 是直接相加，会重复计算同时扮演两种角色的模型。"),
        "n_models_without_base": int(n_models - n_derived),
        "n_self_loop_edges": int(n_self_loops),
        "n_self_loop_nodes": int(n_self_loop_nodes),
        "self_loop_note": (
            "src == dst 的自环（模型把自己列为自己的 base model）是真实存在的数据噪声。"
            "本分析**保留**这些边、不做静默删除；但它们会让该节点单独构成一个规模为 1 的"
            "弱连通分量，解释分量规模分布时需知悉。"),
        "n_edges_with_external_parent": int(n_ext_edges),
        "n_distinct_external_parents": int(n_ext_parents),
        "external_edge_share": round(n_ext_edges / n_edges, 6),
        "max_children_count": int(deg_stats["max_out"] or 0),
        "avg_children_count": round(float(deg_stats["avg_out"] or 0), 4),
        "max_descendant_count_khop": int(desc_stats["max_desc"] or 0),
        "avg_descendant_count_khop": round(float(desc_stats["avg_desc"] or 0), 4),
        "reach_hops": REACH_HOPS,
        "relation_counts": {x["relation"]: int(x["n_edges"]) for x in rel.collect()},
        "top10_bases_by_children": top_fam_pdf.head(10).to_dict(orient="records"),
        "seconds": round(time.time() - t0, 1),
    }
    with open(os.path.join(ANALYSIS, "graph_stats.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
