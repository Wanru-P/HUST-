#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis_graph_gf.py — RQ3：用 **GraphFrames 真正执行**迭代图算法。

与 `analysis_graph.py` 的分工：
    analysis_graph.py    Spark 原生、精确、必定可完成（出度/入度/relation/外部父模型/≤K 跳可达）
    本脚本               GraphFrames 迭代算法（degrees / connectedComponents / pageRank / labelPropagation）

**诚实原则**：本机 32GB、local[6]、driver 3g 是有限资源。若某项迭代算法跑不完，
就由 `run_graph_analysis.py` 杀掉进程并在 `graphframes_result.json` 里记录
`status = "timeout"`，**绝不**改用 NetworkX 结果冒充 GraphFrames 结果。

PageRank 的方向解释（必须明确，否则结论不成立）：
    forward 图（base → derived）：高分的节点是"衍生链的末端"，不是"基础模型影响力"。
    reverse 图（derived → base）：高分的节点才是"被大量模型当作底座依赖"的中心，
        这个才是通常想说的"基础模型影响力"。两个都要跑、分别解释。
    **不允许**把 PageRank 直接等同于模型质量。

用法：python analysis_graph_gf.py
"""
import json
import os
import threading
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(DIRS["gold"], "analysis")
CKPT = os.path.join(PROJECT_ROOT, "data_hubstats", "_smoke", "gf_checkpoint")
RESULT = os.path.join(ANALYSIS, "graphframes_result.json")
os.makedirs(ANALYSIS, exist_ok=True)

VIZ_MAX_NODES = int(os.environ.get("HF_VIZ_MAX_NODES", "1500"))
# 本机资源有限，迭代算法可能跑不完。超时看门狗到点就把**已完成步骤**落盘并退出，
# 让 run_graph_analysis.py 能如实记录 status=timeout，而不是挂死或谎报完成。
BUDGET_SEC = int(os.environ.get("HF_GF_BUDGET_SEC", "2400"))

RESULT_STATE = {"generated_at": datetime.now(timezone.utc).isoformat(),
                "engine": "graphframes",
                "graphframes_version": SA.GRAPHFRAMES_VERSION,
                "snapshot_sha": SA.HUBSTATS_COMMIT,
                "master": SS.DEFAULT_MASTER, "driver_memory": SS.DEFAULT_DRIVER_MEMORY,
                "budget_seconds": BUDGET_SEC,
                "steps": {}, "status": "running"}


def flush_result():
    """每一步都落盘一次，保证任何时刻磁盘上都有最新状态。"""
    with open(RESULT, "w", encoding="utf-8") as f:
        json.dump(RESULT_STATE, f, ensure_ascii=False, indent=2, default=str)


def watchdog_worker():
    """超时看门狗线程体（模块级，避免在函数内嵌套定义函数）。"""
    time.sleep(BUDGET_SEC)
    RESULT_STATE["status"] = "timeout"
    RESULT_STATE["timeout_note"] = (
        f"超出 {BUDGET_SEC}s 预算，已中止。上面 steps 里已完成的部分是真实结果；"
        "未出现的步骤表示本机未完成，不是 0 也不是估计值。")
    flush_result()
    print(f"[gf] !! 超出 {BUDGET_SEC}s 预算，中止并落盘部分结果", flush=True)
    os._exit(3)


def start_watchdog():
    threading.Thread(target=watchdog_worker, daemon=True).start()


def log(msg, t0):
    print(f"[gf] {time.time() - t0:7.1f}s  {msg}", flush=True)


def load_prev():
    """读取上一轮的 graphframes_result.json（同快照才算数），用于断点续跑。"""
    if not os.path.exists(RESULT):
        return {}
    try:
        with open(RESULT, encoding="utf-8") as f:
            prev = json.load(f)
    except Exception:
        return {}
    if prev.get("snapshot_sha") != SA.HUBSTATS_COMMIT:
        return {}
    return prev


def can_reuse(prev_steps, step, artifacts):
    """该步上一轮已完成、且产物文件仍在 -> 可以复用，不必重算。

    本机一次全量迭代（CC + 两次 PageRank + 标签传播）约 22 分钟，
    任何后续步骤出错都不应该让前 22 分钟白跑。
    """
    return step in prev_steps and all(os.path.exists(a) for a in artifacts)


def main():
    t0 = time.time()
    start_watchdog()
    spark = SS.build("HF_Analysis_Graph_GF")
    os.makedirs(CKPT, exist_ok=True)
    spark.sparkContext.setCheckpointDir(CKPT)

    prev = load_prev()
    prev_steps = prev.get("steps", {})
    if prev_steps:
        RESULT_STATE["generated_at"] = prev.get("generated_at",
                                                 RESULT_STATE["generated_at"])
        RESULT_STATE["resumed_from"] = prev.get("generated_at")
        RESULT_STATE["resumed_steps"] = []

    edges = spark.read.parquet(os.path.join(SILVER, "model_base_edges")) \
        .select("src", "dst", "relation")
    models = spark.read.parquet(os.path.join(SILVER, "models")) \
        .select("model_id", "downloads", "downloadsAllTime", "pipeline_tag")

    n_models = models.count()
    # 只把**参与边的节点**放进图；其余模型是孤立的单点分量，可解析计数，无需进图。
    involved = (edges.select(F.col("src").alias("id"))
                .unionByName(edges.select(F.col("dst").alias("id"))).distinct())
    vertices = involved.join(models, involved["id"] == models["model_id"], how="left") \
        .select(F.col("id"), "downloads", "downloadsAllTime", "pipeline_tag")
    n_v = vertices.count()
    n_e = edges.count()
    log(f"图规模：顶点 {n_v:,} / 边 {n_e:,}（全量模型 {n_models:,}）", t0)

    from graphframes import GraphFrame
    gf = GraphFrame(vertices, edges.select("src", "dst"))
    result = RESULT_STATE
    result["n_vertices_in_graph"] = int(n_v)
    result["n_edges"] = int(n_e)
    result["n_models_total"] = int(n_models)
    result["isolated_models_not_in_graph"] = int(n_models - n_v)
    flush_result()

    # ---- 1. degrees ----
    p = os.path.join(ANALYSIS, "gf_degrees")
    if can_reuse(prev_steps, "degrees", [p]):
        deg = spark.read.parquet(p).select("id", "degree")
        result["steps"]["degrees"] = prev_steps["degrees"]
        result["resumed_steps"].append("degrees")
        log(f"degrees 复用上轮结果 {prev_steps['degrees']}", t0)
    else:
        t = time.time()
        deg = gf.degrees
        deg.write.mode("overwrite").parquet(p)
        result["steps"]["degrees"] = {"seconds": round(time.time() - t, 1),
                                      "rows": int(deg.count())}
        log(f"degrees 完成 {result['steps']['degrees']}", t0)
    flush_result()

    # ---- 2. connectedComponents（弱连通分量）----
    p = os.path.join(ANALYSIS, "gf_components")
    if can_reuse(prev_steps, "connected_components", [p]):
        cc = spark.read.parquet(p).select("id", "component")
        result["steps"]["connected_components"] = prev_steps["connected_components"]
        result["resumed_steps"].append("connected_components")
        log(f"connectedComponents 复用上轮结果 "
            f"{prev_steps['connected_components']}", t0)
    else:
        t = time.time()
        cc = gf.connectedComponents()
        cc.write.mode("overwrite").parquet(p)
        n_comp_in_graph = cc.select("component").distinct().count()
        result["steps"]["connected_components"] = {
            "seconds": round(time.time() - t, 1),
            "components_in_graph": int(n_comp_in_graph),
            # 图外的孤立模型每个自成 1 个单点分量
            "components_total": int(n_comp_in_graph + (n_models - n_v))}
        log(f"connectedComponents 完成 {result['steps']['connected_components']}", t0)
    flush_result()

    size_dist = (cc.groupBy("component").agg(F.count("*").cast("long").alias("component_size"))
                 .groupBy("component_size").agg(F.count("*").cast("long").alias("num_components"))
                 .orderBy("component_size"))
    size_dist.toPandas().to_csv(os.path.join(ANALYSIS, "component_size_dist.csv"), index=False)
    largest = (cc.groupBy("component").agg(F.count("*").cast("long").alias("component_size"))
               .orderBy(F.desc("component_size")).limit(20).toPandas())
    largest.to_csv(os.path.join(ANALYSIS, "largest_components.csv"), index=False)

    # ---- 3. PageRank（forward：base -> derived）----
    p = os.path.join(ANALYSIS, "gf_pagerank_forward")
    if can_reuse(prev_steps, "pagerank_forward", [p]):
        prf = spark.read.parquet(p).select("id", "pagerank_forward")
        result["steps"]["pagerank_forward"] = prev_steps["pagerank_forward"]
        result["resumed_steps"].append("pagerank_forward")
        log(f"pageRank(forward) 复用上轮结果 {prev_steps['pagerank_forward']}", t0)
    else:
        t = time.time()
        pr_f = gf.pageRank(resetProbability=0.15, maxIter=10)
        prf = pr_f.vertices.select("id", F.col("pagerank").alias("pagerank_forward"))
        prf.write.mode("overwrite").parquet(p)
        result["steps"]["pagerank_forward"] = {"seconds": round(time.time() - t, 1)}
        log(f"pageRank(forward) 完成 {result['steps']['pagerank_forward']}", t0)
    flush_result()

    # ---- 4. PageRank（reverse：derived -> base）—— 这才是"被依赖程度" ----
    p = os.path.join(ANALYSIS, "gf_pagerank_reverse")
    if can_reuse(prev_steps, "pagerank_reverse", [p]):
        prr = spark.read.parquet(p).select("id", "pagerank_reverse")
        result["steps"]["pagerank_reverse"] = prev_steps["pagerank_reverse"]
        result["resumed_steps"].append("pagerank_reverse")
        log(f"pageRank(reverse) 复用上轮结果 {prev_steps['pagerank_reverse']}", t0)
    else:
        t = time.time()
        gf_rev = GraphFrame(vertices,
                            edges.select(F.col("dst").alias("src"), F.col("src").alias("dst")))
        pr_r = gf_rev.pageRank(resetProbability=0.15, maxIter=10)
        prr = pr_r.vertices.select("id", F.col("pagerank").alias("pagerank_reverse"))
        prr.write.mode("overwrite").parquet(p)
        result["steps"]["pagerank_reverse"] = {"seconds": round(time.time() - t, 1)}
        log(f"pageRank(reverse) 完成 {result['steps']['pagerank_reverse']}", t0)
    flush_result()

    # ---- 5. labelPropagation（社区）----
    p = os.path.join(ANALYSIS, "gf_communities")
    if can_reuse(prev_steps, "label_propagation", [p]):
        lp = spark.read.parquet(p).select("id", "community_label")
        result["steps"]["label_propagation"] = prev_steps["label_propagation"]
        result["resumed_steps"].append("label_propagation")
        log(f"labelPropagation 复用上轮结果 {prev_steps['label_propagation']}", t0)
    else:
        t = time.time()
        lp_raw = gf.labelPropagation(maxIter=5)
        lp = lp_raw.select("id", F.col("label").cast("long").alias("community_label"))
        lp.write.mode("overwrite").parquet(p)
        result["steps"]["label_propagation"] = {
            "seconds": round(time.time() - t, 1),
            "n_communities": int(lp.select("community_label").distinct().count())}
        log(f"labelPropagation 完成 {result['steps']['label_propagation']}", t0)
    flush_result()

    # ---- 6. 合并逐节点指标 ----
    # 注意：pr_f.vertices / pr_r.vertices 的列名是 pagerank，别名只存在于上面的
    # 写出表达式里。这里统一使用已经改好名的 prf / prr，避免再次踩列名不匹配。
    t = time.time()
    metrics = (vertices
               .join(deg, on="id", how="left")
               .join(cc, on="id", how="left")
               .join(prf, on="id", how="left")
               .join(prr, on="id", how="left")
               .join(lp, on="id", how="left")
               .join(cc.groupBy("component").agg(F.count("*").cast("long")
                                                 .alias("component_size")),
                     on="component", how="left"))
    metrics.write.mode("overwrite").parquet(os.path.join(ANALYSIS, "node_metrics_gf"))
    result["steps"]["merge_node_metrics"] = {"seconds": round(time.time() - t, 1),
                                             "rows": int(n_v)}
    log(f"合并逐节点指标完成 {result['steps']['merge_node_metrics']}", t0)
    flush_result()

    # ---- 7. 可视化子图：取最大连通分量内度数最高的 N 个节点 ----
    big = largest.iloc[0]["component"] if len(largest) else None
    viz = {"nodes": [], "links": [], "component": None, "truncated": False}
    if big is not None:
        comp_nodes = cc.filter(F.col("component") == big).select("id")
        picked = (comp_nodes.join(deg, on="id")
                  .orderBy(F.desc("degree"), F.asc("id")).limit(VIZ_MAX_NODES)
                  .select("id"))
        ids = [r["id"] for r in picked.collect()]
        idset = set(ids)
        sub = edges.filter(F.col("src").isin(ids) & F.col("dst").isin(ids)).collect()
        n_comp_size = int(largest.iloc[0]["component_size"])
        viz["component"] = int(big)
        viz["component_size"] = n_comp_size
        viz["truncated"] = bool(n_comp_size > VIZ_MAX_NODES)
        viz["nodes"] = ids
        viz["links"] = [{"src": r["src"], "dst": r["dst"]} for r in sub]
    with open(os.path.join(ANALYSIS, "family_graph.json"), "w", encoding="utf-8") as f:
        json.dump(viz, f, ensure_ascii=False)
    result["viz_subgraph"] = {"component": viz["component"],
                              "component_size": viz.get("component_size"),
                              "n_nodes_shown": len(viz["nodes"]),
                              "n_links_shown": len(viz["links"]),
                              "truncated": viz["truncated"]}

    result["status"] = "completed"
    result["total_seconds"] = round(time.time() - t0, 1)
    if result.get("resumed_steps"):
        result["resume_note"] = (
            "下列步骤复用了上一轮**同快照**已落盘的 parquet 结果，未重算："
            + "、".join(result["resumed_steps"])
            + "。其余步骤为本次实际计算。复用只是省去重复迭代，数据与算法完全一致。")
    flush_result()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
