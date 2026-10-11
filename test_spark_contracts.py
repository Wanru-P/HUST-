#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_spark_contracts.py — Spark 侧数据契约与算法正确性测试（Phase 5 验收项）。

纯计算部分的等价性由 `test_algorithms.py` 覆盖（不需要 Spark）。
本文件专门验证**只有真实数据才能验证**的契约：

  A. 规模与守恒
     - Silver 主表 == Gold 宽表 == download_distribution == model_lifecycle
       == model_adoption_features 行数完全一致
     - task_monthly_stats / library_monthly_stats 的 new_models 求和 == 主表行数
     - model_id 无重复；gated 全为 NULL（新数据源无该字段）
  B. 时间契约
     - created_at / last_modified 全部可解析、全部符合 ISO-8601 UTC（带 Z）
     - created_month == date_format(created_ts, 'yyyy-MM')，month_num ∈ [1,12]
  C. 边方向契约（**最关键**）
     - 抽样：取一个已知有 base model 的模型，验证边是 (src=底座, dst=该模型)，不能反向
     - 所有 dst 都能在 Silver 主表里找到
     - 外部父模型（src 不在主表里）**被保留**，没有被静默丢弃
  D. family_size 语义
     - family_size == children_count + 1（全部行）
     - children_count == 以该模型为 src 的边数
  E. 集中度口径
     - download_distribution 的 rank 是 1..n 的一个排列（无重复、无缺口）
     - 由 download_distribution 逐模型口径重算的 Gini/Top-k
       与 concentration_stats.json 的频数加权口径在 1e-9 内一致
  F. 图节点覆盖
     - 每条边的 src / dst 都出现在 node_metrics 中
     - n_nodes_involved(并集) == graphframes_result.json 的 n_vertices_in_graph
     - node_metrics.direct_family_size == children_count + 1
  G. RQ4 / RQ5 结论文件自洽
     - ml_summary.json：train+test == n_sample、overlap == 0、seed 固定
     - rq5_summary.json：comparable 为真时 n_models ≥ 阈值
  H. 选样一致性（**跨引擎可比性的前提**）
     - Python zlib.crc32 与 Spark pmod(F.crc32(id),100) 在真实 model_id 上
       选中**同一批行**；否则 local 与 spark 的规模档根本不可比。

运行：python test_spark_contracts.py
输出：data_hubstats/gold/analysis/contract_test_result.json + 控制台 PASS/FAIL
"""
import json
import os
import re
import sys
import zlib
from datetime import datetime, timezone

from pyspark.sql import functions as F

import schema_adapter as SA
import spark_session as SS
import stats_utils as SU

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
GOLD = DIRS["gold"]
SILVER = DIRS["silver"]
ANALYSIS = os.path.join(GOLD, "analysis")
OUT = os.path.join(ANALYSIS, "contract_test_result.json")

ISO_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
RESULTS = []


def check(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""),
          flush=True)
    RESULTS.append({"test": name, "ok": bool(ok), "detail": str(detail)})
    return bool(ok)


def read_json(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    spark = SS.build("HF_Contract_Tests")

    silver_models = spark.read.parquet(os.path.join(SILVER, "models"))
    edges = spark.read.parquet(os.path.join(SILVER, "model_base_edges"))
    enriched = spark.read.parquet(os.path.join(GOLD, "models_enriched"))
    n_silver = silver_models.count()
    n_enriched = enriched.count()

    # ---------------- A. 规模与守恒 ----------------
    print("\n== A. 规模与守恒 ==")
    sizes = {}
    for name in ("download_distribution", "model_lifecycle", "model_adoption_features"):
        sizes[name] = spark.read.parquet(os.path.join(GOLD, name)).count()
    check("Silver 主表行数 == Gold 宽表行数", n_silver == n_enriched,
          f"silver={n_silver} enriched={n_enriched}")
    for name, cnt in sizes.items():
        check(f"{name} 行数 == 主表行数", cnt == n_silver, f"{cnt} vs {n_silver}")

    dup = silver_models.groupBy("model_id").count().filter(F.col("count") > 1).count()
    check("model_id 无重复", dup == 0, f"重复组数={dup}")

    gated_nonnull = silver_models.filter(F.col("gated").isNotNull()).count()
    check("gated 全为 NULL（新数据源无该字段，未被推断）", gated_nonnull == 0,
          f"非空={gated_nonnull}")

    for table, key in (("task_monthly_stats", "task"), ("library_monthly_stats", "library")):
        s = (spark.read.parquet(os.path.join(GOLD, table))
             .agg(F.sum("new_models")).first()[0])
        check(f"{table}.new_models 求和 == 主表行数（守恒）", int(s) == n_silver,
              f"{s} vs {n_silver}")
    tag_sum = spark.read.parquet(os.path.join(GOLD, "tag_monthly_stats")) \
        .agg(F.sum("new_models")).first()[0]
    check("tag_monthly_stats 求和 ≥ 主表行数（一对多，允许大于）", int(tag_sum) >= n_silver,
          f"{tag_sum} vs {n_silver}")

    # ---------------- B. 时间契约 ----------------
    print("\n== B. 时间契约 ==")
    bad_iso = silver_models.filter(
        ~F.col("created_at").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
    ).count()
    check("created_at 全部符合 ISO-8601 UTC（带 Z）", bad_iso == 0, f"不合规={bad_iso}")
    bad_lm = silver_models.filter(
        F.col("last_modified").isNotNull()
        & ~F.col("last_modified").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
    ).count()
    check("last_modified 全部符合 ISO-8601 UTC（带 Z）", bad_lm == 0, f"不合规={bad_lm}")

    parse_fail = enriched.filter(F.to_timestamp("created_at").isNull()).count()
    check("created_at 可被 to_timestamp 解析", parse_fail == 0, f"失败={parse_fail}")

    mismatch = enriched.filter(
        F.col("created_month") != F.date_format(F.to_timestamp("created_at"), "yyyy-MM")
    ).count()
    check("created_month == date_format(created_ts,'yyyy-MM')", mismatch == 0,
          f"不一致={mismatch}")

    bad_month = enriched.filter(~F.col("month_num").between(1, 12)).count()
    check("month_num ∈ [1,12]", bad_month == 0, f"越界={bad_month}")

    # UTC 时区生效检验：东八区会把 23:xx 的 UTC 记录推到次月
    spark_tz = spark.conf.get("spark.sql.session.timeZone")
    check("spark.sql.session.timeZone == UTC", spark_tz == "UTC", f"当前={spark_tz}")

    # ---------------- C. 边方向契约 ----------------
    print("\n== C. 边方向契约 ==")
    sample = (silver_models
              .filter(F.size(F.col("base_models")) > 0)
              .select("model_id", "base_models").limit(5).collect())
    direction_ok = True
    for r in sample:
        for base in r["base_models"]:
            hit = edges.filter((F.col("src") == base) & (F.col("dst") == r["model_id"])).count()
            if hit < 1:
                direction_ok = False
                print(f"    缺少边 {base} -> {r['model_id']}")
    check("抽样验证：边为 (src=底座, dst=衍生模型)，方向未反", direction_ok,
          f"抽样 {len(sample)} 个模型")

    model_ids = silver_models.select(F.col("model_id").alias("dst"))
    orphan_dst = edges.join(model_ids, on="dst", how="left_anti").count()
    check("所有 dst 均存在于 Silver 主表", orphan_dst == 0, f"孤儿 dst 边={orphan_dst}")

    ext = edges.join(silver_models.select(F.col("model_id").alias("src")), on="src",
                     how="left_anti")
    n_ext = ext.count()
    check("外部父模型边被保留（未静默丢弃）", n_ext > 0, f"外部边={n_ext}")
    n_edges = edges.count()
    check("外部边占比 < 10%（合理范围）", n_ext / n_edges < 0.10,
          f"{n_ext / n_edges:.6f}")

    # ---------------- D. family_size 语义 ----------------
    print("\n== D. family_size 语义 ==")
    bad_fam = enriched.filter(F.col("family_size") != F.col("children_count") + 1).count()
    check("family_size == children_count + 1（全部行）", bad_fam == 0, f"不一致={bad_fam}")

    out_deg = edges.groupBy("src").agg(F.count("*").cast("long").alias("c"))
    recomputed = (enriched.select("model_id", "children_count")
                  .join(out_deg, enriched["model_id"] == out_deg["src"], how="left")
                  .withColumn("c", F.coalesce("c", F.lit(0))))
    bad_deg = recomputed.filter(F.col("children_count") != F.col("c")).count()
    check("children_count == 以该模型为 src 的边数", bad_deg == 0, f"不一致={bad_deg}")

    # ---------------- E. 集中度口径 ----------------
    print("\n== E. 集中度口径 ==")
    dd = spark.read.parquet(os.path.join(GOLD, "download_distribution"))
    rank_agg = dd.agg(F.min("rank").alias("lo"), F.max("rank").alias("hi"),
                      F.countDistinct("rank").alias("n_distinct"),
                      F.count("*").alias("n_rows")).first()
    check("download_distribution.rank 是 1..n 的排列",
          int(rank_agg["lo"]) == 1 and int(rank_agg["hi"]) == n_silver
          and int(rank_agg["n_distinct"]) == n_silver and int(rank_agg["n_rows"]) == n_silver,
          f"min={rank_agg['lo']} max={rank_agg['hi']} distinct={rank_agg['n_distinct']} "
          f"rows={rank_agg['n_rows']}")

    cs = read_json(os.path.join(GOLD, "concentration_stats.json"))
    if cs is None:
        check("concentration_stats.json 存在", False, "文件缺失")
    else:
        for metric in ("downloads", "downloadsAllTime"):
            freq_rows = (enriched.groupBy(F.col(metric).cast("long").alias("v"))
                         .agg(F.count("*").cast("long").alias("c"))
                         .orderBy("v").collect())
            vals = [int(r["v"]) for r in freq_rows]
            freqs = [int(r["c"]) for r in freq_rows]
            st = SU.concentration_stats(vals, freqs)
            ref = cs[metric]
            check(f"Gini({metric}) 由主表重算 == concentration_stats.json",
                  abs(st["gini"] - ref["gini"]) < 1e-9,
                  f"重算={st['gini']} 文件={ref['gini']}")
            for k in ("top_1pct_share", "top_5pct_share", "top_10pct_share"):
                a, b = st[k], ref[k]
                check(f"{k}({metric}) 一致",
                      (a is None and b is None) or abs(a - b) < 1e-9, f"{a} vs {b}")
            check(f"zero_value_models({metric}) 一致",
                  st["zero_value_models"] == ref["zero_value_models"],
                  f"{st['zero_value_models']} vs {ref['zero_value_models']}")
            check(f"total_downloads({metric}) 一致",
                  st["total_downloads"] == ref["total_downloads"],
                  f"{st['total_downloads']} vs {ref['total_downloads']}")

    # ---------------- F. 图节点覆盖 ----------------
    print("\n== F. 图节点覆盖 ==")
    nm_path = os.path.join(ANALYSIS, "node_metrics")
    if not os.path.exists(nm_path):
        check("node_metrics 存在", False, "未运行 analysis_graph.py")
    else:
        nm = spark.read.parquet(nm_path)
        n_nm = nm.count()
        check("node_metrics 行数 == 主表行数（覆盖全部模型）", n_nm == n_silver,
              f"{n_nm} vs {n_silver}")
        nm_src = nm.select(F.col("model_id").alias("src"))
        miss_src_df = edges.join(nm_src, on="src", how="left_anti")
        miss_src = miss_src_df.count()
        nm_dst = nm.select(F.col("model_id").alias("dst"))
        miss_dst = edges.join(nm_dst, on="dst", how="left_anti").count()
        check("每条边的 dst 都在 node_metrics 中", miss_dst == 0, f"缺失={miss_dst}")

        bad_dfs = nm.filter(F.col("direct_family_size") != F.col("children_count") + 1).count()
        check("node_metrics.direct_family_size == children_count + 1", bad_dfs == 0,
              f"不一致={bad_dfs}")

        gs = read_json(os.path.join(ANALYSIS, "graph_stats.json"))
        if gs:
            check("graph_stats.json 节点数为并集去重（非朴素相加）",
                  gs["n_nodes_involved"] != gs["n_base_plus_derived"]
                  and gs["n_base_plus_derived"] - gs["n_nodes_involved"]
                  == gs["n_nodes_both_base_and_derived"],
                  f"并集={gs['n_nodes_involved']} 相加={gs['n_base_plus_derived']} "
                  f"差={gs['n_nodes_both_base_and_derived']}")

            # src 允许缺失，但**只允许缺失外部父模型**。
            # node_metrics 建立在 Silver 主表之上，而 baseModels 会指向不在主表里的
            # 外部模型（graph_stats.n_edges_with_external_parent）；契约 C 明确要求这些
            # 边被**保留**而非静默丢弃，所以它们必然出现在边表里却不在 node_metrics 里。
            # 因此这里不能断言「所有 src 都命中」，而要断言缺失量**恰好等于**外部父模型
            # 边数、去重缺失量恰好等于外部父模型数 —— 这比原断言更严格：
            # 一旦有主表内的 src 被漏掉，两个等式都会立刻失败。
            ext_edges = gs.get("n_edges_with_external_parent")
            ext_parents = gs.get("n_distinct_external_parents")
            if ext_edges is None or ext_parents is None:
                check("graph_stats.json 记录外部父模型边（用于校验 src 缺失量）", False,
                      "缺少 n_edges_with_external_parent / n_distinct_external_parents")
            else:
                n_miss_src_distinct = miss_src_df.select("src").distinct().count()
                check("src 缺失量 == graph_stats 外部父模型边数（缺失只来自外部父模型）",
                      miss_src == int(ext_edges),
                      f"缺失={miss_src} 外部父模型边={ext_edges}")
                check("src 缺失去重数 == graph_stats 外部父模型数",
                      n_miss_src_distinct == int(ext_parents),
                      f"缺失去重={n_miss_src_distinct} 外部父模型={ext_parents}")

        gfr = read_json(os.path.join(ANALYSIS, "graphframes_result.json"))
        if gfr and gs:
            check("graphframes_result.n_vertices_in_graph == graph_stats.n_nodes_involved",
                  int(gfr["n_vertices_in_graph"]) == int(gs["n_nodes_involved"]),
                  f"{gfr['n_vertices_in_graph']} vs {gs['n_nodes_involved']}")
            check("graphframes_result.n_edges == graph_stats.n_edges",
                  int(gfr["n_edges"]) == int(gs["n_edges"]),
                  f"{gfr['n_edges']} vs {gs['n_edges']}")

    # ---------------- G. RQ4 / RQ5 结论文件自洽 ----------------
    print("\n== G. RQ4 / RQ5 自洽 ==")
    ml = read_json(os.path.join(ANALYSIS, "ml_summary.json"))
    if ml is None:
        check("ml_summary.json 存在", False, "未运行 analysis_ml.py")
    else:
        sp = ml["split"]
        check("RQ4 train+test == 建模样本量",
              sp["n_train"] + sp["n_test"] == ml["sampling"]["n_sample"],
              f"{sp['n_train']}+{sp['n_test']} vs {ml['sampling']['n_sample']}")
        check("RQ4 train∩test == 0", sp["train_test_overlap"] == 0,
              str(sp["train_test_overlap"]))
        check("RQ4 seed 固定为 42", sp["seed"] == 42, str(sp["seed"]))
        check("RQ4 声明无目标派生特征", ml["features"]["no_target_derived_inputs"] is True)
        check("RQ4 含简单基线", "Baseline_mean" in ml["metrics_on_test"],
              str(list(ml["metrics_on_test"])))
        check("RQ4 三种模型指标齐备",
              all(k in ml["metrics_on_test"][m] for m in ml["metrics_on_test"]
                  for k in ("rmse", "mae", "r2")))

    rq5 = read_json(os.path.join(ANALYSIS, "rq5_summary.json"))
    if rq5 is None:
        check("rq5_summary.json 存在", False, "未运行 analysis_rq5.py")
    else:
        thr = rq5["min_group_for_comparison"]
        viol = [r["task"] for r in rq5["rows"] if r["comparable"] and r["n_models"] < thr]
        check("RQ5 comparable 的任务组均满足最小样本阈值", not viol, f"违规={viol}")

        # unknown 是 pipeline_tag 缺失的占位组，不是真实任务：必须单独成行，
        # 且**任何位置**都不能被标成 comparable（否则读 rows 的下游会把
        # 205.8 万条未标注模型当成一个任务参与跨任务比较）。
        unk = rq5["unknown_task"]
        check("RQ5 unknown 单独成行且不参与比较",
              unk is not None and unk["is_unknown"] is True and unk["comparable"] is False,
              f"task={unk['task'] if unk else None} "
              f"is_unknown={unk['is_unknown'] if unk else None} "
              f"comparable={unk['comparable'] if unk else None}")
        unk_in_rows = [r for r in rq5["rows"] if r["is_unknown"]]
        check("RQ5 rows 中 unknown 行也为 comparable=False",
              len(unk_in_rows) == 1 and unk_in_rows[0]["comparable"] is False,
              f"unknown 行数={len(unk_in_rows)} "
              f"comparable={unk_in_rows[0]['comparable'] if unk_in_rows else None}")
        # n_tasks_comparable 必须只统计「已标注 且 comparable」的任务组，
        # 与 local 路径同口径 —— 这是两引擎结果一致性的一个具体断言。
        n_expected = sum(1 for r in rq5["rows"] if (not r["is_unknown"]) and r["comparable"])
        check("RQ5 n_tasks_comparable == 已标注且 comparable 的任务组数",
              int(rq5["n_tasks_comparable"]) == n_expected,
              f"{rq5['n_tasks_comparable']} vs {n_expected}")
        check("RQ5 n_tasks_labeled == rows 中非 unknown 行数",
              int(rq5["n_tasks_labeled"]) == sum(1 for r in rq5["rows"] if not r["is_unknown"]),
              f"{rq5['n_tasks_labeled']} vs "
              f"{sum(1 for r in rq5['rows'] if not r['is_unknown'])}")
        check("RQ5 两种下载口径独立成列",
              "gini_downloads" in rq5["rows"][0] and "gini_downloadsAllTime" in rq5["rows"][0])

    # ---------------- H. 选样一致性（跨引擎可比性的前提）----------------
    print("\n== H. 选样一致性（local 与 spark 必须选中同一批行）==")
    ids = [r["model_id"] for r in
           silver_models.select("model_id").limit(200000).collect()]
    py_keep = {s for s in ids if zlib.crc32(s.encode("utf-8")) % 100 < 10}
    sel = F.pmod(F.crc32(F.col("model_id")), F.lit(100)) < 10
    ids_df = spark.createDataFrame([(s,) for s in ids], ["model_id"])
    spark_keep = {r["model_id"] for r in ids_df.filter(sel).select("model_id").collect()}
    check("crc32 选样：Python(zlib) 与 Spark(pmod(F.crc32)) 选中同一批行",
          spark_keep == py_keep,
          f"样本={len(ids)} py={len(py_keep)} spark={len(spark_keep)} "
          f"交集={len(spark_keep & py_keep)}")
    check("crc32 选样比例接近 10%", abs(len(py_keep) / max(1, len(ids)) - 0.10) < 0.01,
          f"rate={len(py_keep) / max(1, len(ids)):.5f}")

    spark.stop()

    n_fail = sum(1 for r in RESULTS if not r["ok"])
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(),
               "n_tests": len(RESULTS), "n_failed": n_fail, "results": RESULTS}
    os.makedirs(ANALYSIS, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\n共 {len(RESULTS)} 项测试，失败 {n_fail} 项 -> {OUT}")
    if n_fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
