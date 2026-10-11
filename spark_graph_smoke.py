#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spark_graph_smoke.py — GraphFrames 最小可用性验证（Phase 2 RQ3 前置）。

目的：**不能只确认 JAR 存在就宣布兼容**。本脚本在一张 20 节点 / 26 边的小图上
真正执行一遍 GraphFrames 的迭代算法，确认：
  1. GraphFrame(vertices, edges) 构造成功
  2. degrees（度数）
  3. connectedComponents（连通分量，迭代算法）
  4. pageRank（迭代算法）
  5. labelPropagation（迭代算法）
  6. 结果可回读、可写出

产出：data_hubstats/_smoke/graphframes_smoke_result.json
运行：python spark_graph_smoke.py
"""
import json
import os
import time

from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(PROJECT_ROOT, "data_hubstats", "_smoke", "graphframes_smoke_result.json")
CKPT = os.path.join(PROJECT_ROOT, "data_hubstats", "_smoke", "gf_checkpoint")


def main():
    report = {"master": SS.DEFAULT_MASTER, "driver_memory": SS.DEFAULT_DRIVER_MEMORY,
              "graphframes_version": SA.GRAPHFRAMES_VERSION,
              "jars": [os.path.basename(p) for p in SA.GRAPHFRAMES_JARS]}

    spark = SS.build("HF_GraphFrames_Smoke", shuffle_partitions=4)
    spark.sparkContext.setCheckpointDir(CKPT)
    os.makedirs(CKPT, exist_ok=True)

    # 小图：3 个星形家族 + 1 条链 + 2 个孤立点
    #   base0 -> d1..d5 ; base1 -> d6..d10 ; base2 -> d11..d14 ; d14 -> d15 ; 孤立 x1, x2
    edges = [(f"base0", f"d{i}") for i in range(1, 6)]
    edges += [(f"base1", f"d{i}") for i in range(6, 11)]
    edges += [(f"base2", f"d{i}") for i in range(11, 15)]
    edges += [("d14", "d15")]
    vertices = sorted({v for e in edges for v in e} | {"x1", "x2"})

    v = spark.createDataFrame([(x,) for x in vertices], ["id"])
    e = spark.createDataFrame([(s, d) for s, d in edges], ["src", "dst"])

    t0 = time.time()
    from graphframes import GraphFrame
    gf = GraphFrame(v, e)
    report["n_vertices"] = gf.vertices.count()
    report["n_edges"] = gf.edges.count()
    report["construct_seconds"] = round(time.time() - t0, 2)

    t1 = time.time()
    deg = gf.degrees
    report["degrees_top"] = (deg.orderBy(F.desc("degree"), F.asc("id")).limit(3)
                             .toPandas().to_dict(orient="records"))
    report["degrees_seconds"] = round(time.time() - t1, 2)

    t2 = time.time()
    cc = gf.connectedComponents()
    report["n_components"] = cc.select("component").distinct().count()
    report["components_seconds"] = round(time.time() - t2, 2)

    t3 = time.time()
    pr = gf.pageRank(resetProbability=0.15, maxIter=10)
    report["pagerank_top"] = (pr.vertices.orderBy(F.desc("pagerank")).limit(3)
                              .toPandas().to_dict(orient="records"))
    report["pagerank_seconds"] = round(time.time() - t3, 2)

    t4 = time.time()
    # 注意：0.12.3 的 labelPropagation 直接返回**顶点 DataFrame**（不是 GraphFrame），
    # 所以这里没有 .vertices，直接 select。
    lp = gf.labelPropagation(maxIter=5)
    report["n_labels"] = lp.select("label").distinct().count()
    report["labelprop_seconds"] = round(time.time() - t4, 2)

    # 期望值校验（小图可手算）
    report["expect_n_vertices"] = len(vertices)
    report["expect_n_edges"] = len(edges)
    report["expect_n_components"] = 3 + 2   # 3 个家族 + 2 个孤立点
    report["ok"] = bool(report["n_vertices"] == len(vertices)
                        and report["n_edges"] == len(edges)
                        and report["n_components"] == 5)

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
