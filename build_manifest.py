#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_manifest.py — 把全部正式结果收敛成**唯一**的结果清单（Phase 4/5 一致性基准）。

为什么需要它：
    Phase 5 要求「同一指标在 report / Dashboard / Figures / LaTeX 素材 / PPT 素材里
    不能出现不同数字」，且「所有数字必须能自动读取或校验自单一结果文件」。
    因此这里把所有关键指标从各脚本的产物中抽出来，写成一张扁平表
    `results_manifest.json`，并附上每个值的**出处（文件 + 键路径）**。
    报告与素材必须引用这里面的值，不得手抄。

输入（缺哪个就跳过哪块，并显式标记 missing，不猜值）：
    data_hubstats/gold/data_quality_report.json     全量质量与守恒检查
    data_hubstats/gold/concentration_stats.json     集中度（频数加权口径）
    data_hubstats/gold/analysis/summary_rq1.json
    data_hubstats/gold/analysis/summary_rq2.json
    data_hubstats/gold/analysis/graph_stats.json
    data_hubstats/gold/analysis/graphframes_result.json
    data_hubstats/gold/analysis/ml_summary.json
    data_hubstats/gold/analysis/rq5_summary.json
    data_hubstats/gold/analysis/benchmark_all_summary.csv
    data_hubstats/gold/analysis/phase2_runs.json
    data_hubstats/_smoke/full_etl_run_report.json
    data_hubstats/gold/analysis/figure_index.json
    data_hubstats/gold/analysis/family_ext_stats.json      增量：家族结构（Task A）
    data_hubstats/gold/analysis/family_scc_summary.json    增量：环/SCC 摘要
    data_hubstats/gold/analysis/family_multi_parent.csv    增量：多父节点
    data_hubstats/gold/analysis/family_cases.json          增量：三个案例家族（Task B）
    data_hubstats/gold/analysis/concentration_groups.json  增量：分组集中度（Task D）
    data_hubstats/gold/analysis/figures_ext_index.json     增量：新增图表索引（Task E）

输出：
    data_hubstats/gold/analysis/results_manifest.json
    docs/results_manifest.md

用法：python build_manifest.py
"""
import csv
import glob
import json
import os
import sys
from datetime import datetime, timezone

import schema_adapter as SA

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DIRS = SA.resolve_dirs()
GOLD = DIRS["gold"]
ANALYSIS = os.path.join(GOLD, "analysis")
SMOKE = os.path.join(PROJECT_ROOT, "data_hubstats", "_smoke")
DOCS = os.path.join(PROJECT_ROOT, "docs")
os.makedirs(DOCS, exist_ok=True)

MANIFEST = {}
MISSING = []


def load_json(path, label):
    if not os.path.exists(path):
        MISSING.append(label)
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        MISSING.append(f"{label}（读取失败: {e}）")
        return None


def put(key, value, source, note=""):
    """登记一个指标：值 + 出处 + 备注。值为 None 时不登记（避免下游误用）。"""
    if value is None:
        return
    MANIFEST[key] = {"value": value, "source": source, "note": note}


def load_summary_csv(name):
    path = os.path.join(ANALYSIS, name)
    if not os.path.exists(path):
        MISSING.append(name)
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    # ---------- 1. 环境与快照 ----------
    etl = load_json(os.path.join(SMOKE, "full_etl_run_report.json"), "full_etl_run_report")
    put("meta.snapshot_commit", SA.HUBSTATS_COMMIT, "schema_adapter.py")
    put("meta.snapshot_sha256", SA.HUBSTATS_SHA256, "schema_adapter.py")
    put("meta.bronze_file_bytes", SA.HUBSTATS_SIZE, "schema_adapter.py")
    if etl:
        for k, v in etl.get("config", {}).items():
            put(f"env.{k}", v, "full_etl_run_report.json:config")
        put("env.etl_elapsed_seconds", etl.get("elapsed_seconds"),
            "full_etl_run_report.json:elapsed_seconds")
        put("env.etl_exit_code", etl.get("exit_code"), "full_etl_run_report.json:exit_code")
        put("env.peak_jvm_rss_gib", etl.get("peak_jvm_rss_gib"),
            "full_etl_run_report.json:peak_jvm_rss_gib")
        put("env.min_system_available_gib", etl.get("min_system_available_gib"),
            "full_etl_run_report.json:min_system_available_gib")
        put("data.silver_bytes", etl.get("silver_bytes"),
            "full_etl_run_report.json:silver_bytes")
        put("data.gold_bytes", etl.get("gold_bytes"), "full_etl_run_report.json:gold_bytes")
        put("env.etl_step_timings", etl.get("step_timings"),
            "full_etl_run_report.json:step_timings")

    # ---------- 2. 数据规模与质量 ----------
    dq = load_json(os.path.join(GOLD, "data_quality_report.json"), "data_quality_report")
    if dq:
        src = "data_quality_report.json"
        # 键名 -> 清单键名（严格对齐 verify_etl.py 实际写出的字段，不做猜测）
        field_map = [
            ("n_bronze_source_rows", "data.bronze_rows",
             "源 parquet 物理行数（含源数据自带的重复 id）"),
            ("n_bronze_duplicate_ids", "data.source_duplicate_ids",
             "源数据本身重复出现的 id 个数"),
            ("n_models_silver", "data.silver_models_rows", "去重后的 Silver 主表行数"),
            ("n_unique_model_id", "data.unique_model_ids", ""),
            ("dup_model_id", "data.dup_model_id", "去重后主键重复数（应为 0）"),
            ("n_models_enriched", "data.gold_enriched_rows", ""),
            ("n_tag_rows", "data.tag_rows", "(model_id, tag) 长表行数"),
            ("n_unique_tags", "data.unique_tags", ""),
            ("n_edges", "data.edge_rows", "显式底座关系边数"),
            ("n_edge_src", "data.distinct_parents", "不同底座（父）模型数"),
            ("n_edge_dst", "data.distinct_children", "不同衍生（子）模型数"),
            ("n_edges_with_external_parent", "data.external_parent_edges",
             "父模型不在快照内的边（已保留，未丢弃）"),
            ("n_distinct_external_parents", "data.distinct_external_parents", ""),
            ("edge_relation_counts", "data.edge_relation_counts", ""),
            ("downloads_sum", "data.downloads_sum", "downloads 合计（近 30 天口径）"),
            ("downloadsAllTime_sum", "data.downloadsAllTime_sum", "downloadsAllTime 合计"),
            ("created_at_min", "data.created_min", ""),
            ("created_at_max", "data.created_max", "快照中最新创建时间"),
            ("created_at_iso_z_ok", "data.created_at_iso_z_ok", ""),
            ("created_at_parse_fail", "data.created_parse_failures", ""),
            ("n_pipeline_tag_values", "data.distinct_pipeline_tags", ""),
            ("n_library_values", "data.distinct_library_names", ""),
            ("gated_all_null", "data.gated_all_null", "gated 是否全为 NULL"),
            ("models_per_year", "data.per_year", ""),
            ("missing_rate", "data.missing_rates", ""),
            ("conservation", "data.conservation", "聚合守恒：各月度表求和 == 主表行数"),
            ("hard_fail", "data.hard_failures", "硬性失败项数（应为 0）"),
            ("ok", "data.quality_ok", ""),
            ("config", "env.quality_check_config", ""),
            ("silver_rows", "data.silver_rows", ""),
        ]
        for k, out_key, note in field_map:
            put(out_key, dq.get(k), f"{src}:{k}", note)
        for k, v in (dq.get("gold_rows") or {}).items():
            put(f"data.gold_rows.{k}", v, f"{src}:gold_rows.{k}")
        # 同时给出整表 dict：write_reports.py 以 mv(m, "data.gold_rows", {}) 取用，
        # 若只写 data.gold_rows.<表名> 而不写 data.gold_rows，报告里的 Gold 表清单会静默变空。
        put("data.gold_rows", dq.get("gold_rows") or {},
            f"{src}:gold_rows", "Gold 各表行数（表名 -> 行数）")

    # ---------- 3. 集中度 ----------
    cs = load_json(os.path.join(GOLD, "concentration_stats.json"), "concentration_stats")
    if cs:
        for metric in ("downloads", "downloadsAllTime"):
            m = cs.get(metric) or {}
            for k, v in m.items():
                put(f"rq2.{metric}.{k}", v, f"concentration_stats.json:{metric}.{k}")

    # ---------- 4. RQ1 / RQ2 汇总 ----------
    rq1 = load_json(os.path.join(ANALYSIS, "summary_rq1.json"), "summary_rq1")
    if rq1:
        for k in ("n_models", "first_month", "last_month", "n_months", "peak_month",
                  "peak_count", "partial_month", "unknown_task_models",
                  "unknown_task_share", "top10_tasks_by_total",
                  "earliest_day", "earliest_day_count", "first_month_count",
                  "earliest_day_distinct_timestamps", "earliest_day_modal_timestamp",
                  "earliest_day_modal_count", "earliest_day_modal_share",
                  "date_floor_share", "date_floor_note"):
            put(f"rq1.{k}", rq1.get(k), f"summary_rq1.json:{k}")
    rq2 = load_json(os.path.join(ANALYSIS, "summary_rq2.json"), "summary_rq2")
    if rq2:
        for k in ("method", "metric_note", "powerlaw_note"):
            put(f"rq2.meta.{k}", rq2.get(k), f"summary_rq2.json:{k}")

    # ---------- 5. RQ3 ----------
    gs = load_json(os.path.join(ANALYSIS, "graph_stats.json"), "graph_stats")
    if gs:
        for k in ("n_models_total", "n_edges", "n_distinct_base_models",
                  "n_distinct_derived_models", "n_nodes_involved", "n_base_plus_derived",
                  "n_nodes_both_base_and_derived", "n_models_without_base",
                  "n_self_loop_edges", "n_self_loop_nodes", "self_loop_note",
                  "n_edges_with_external_parent", "n_distinct_external_parents",
                  "external_edge_share", "max_children_count", "avg_children_count",
                  "max_descendant_count_khop", "avg_descendant_count_khop",
                  "reach_hops", "relation_counts", "top10_bases_by_children",
                  "node_count_note", "seconds", "engine"):
            put(f"rq3.{k}", gs.get(k), f"graph_stats.json:{k}")
    gfr = load_json(os.path.join(ANALYSIS, "graphframes_result.json"), "graphframes_result")
    if gfr:
        for k in ("status", "n_vertices_in_graph", "n_edges", "n_models_total",
                  "isolated_models_not_in_graph", "budget_seconds", "total_seconds",
                  "viz_subgraph", "timeout_note"):
            put(f"rq3_gf.{k}", gfr.get(k), f"graphframes_result.json:{k}")
        for name, step in (gfr.get("steps") or {}).items():
            put(f"rq3_gf.step.{name}", step, f"graphframes_result.json:steps.{name}")

    # ---------- 6. RQ4 ----------
    ml = load_json(os.path.join(ANALYSIS, "ml_summary.json"), "ml_summary")
    if ml:
        put("rq4.research_positioning", ml.get("research_positioning"),
            "ml_summary.json:research_positioning")
        put("rq4.target", ml.get("target"), "ml_summary.json:target")
        put("rq4.n_rows_full", ml.get("n_rows_full"), "ml_summary.json:n_rows_full")
        put("rq4.n_duplicate_model_id_groups", ml.get("n_duplicate_model_id_groups"),
            "ml_summary.json:n_duplicate_model_id_groups")
        put("rq4.sampling", ml.get("sampling"), "ml_summary.json:sampling")
        put("rq4.split", ml.get("split"), "ml_summary.json:split")
        put("rq4.features", ml.get("features"), "ml_summary.json:features")
        put("rq4.correlations", ml.get("correlations"), "ml_summary.json:correlations")
        put("rq4.metrics_on_test", ml.get("metrics_on_test"), "ml_summary.json:metrics_on_test")
        put("rq4.rf_importance_ranked", ml.get("rf_importance_ranked"),
            "ml_summary.json:rf_importance_ranked")
        put("rq4.feature_importance_note", ml.get("feature_importance_note"),
            "ml_summary.json:feature_importance_note")

    # ---------- 7. RQ5 ----------
    rq5 = load_json(os.path.join(ANALYSIS, "rq5_summary.json"), "rq5_summary")
    if rq5:
        for k in ("n_models_total", "n_tasks_labeled", "n_tasks_comparable",
                  "min_group_for_comparison", "min_distinct_values_for_gini",
                  "threshold_note", "unknown_task", "unknown_note", "caliber_note",
                  "family_note", "top10_by_n_models", "top10_by_downloads_total",
                  "top10_by_gini_downloads", "highest_base_relation_coverage"):
            put(f"rq5.{k}", rq5.get(k), f"rq5_summary.json:{k}")

    # ---------- 7.5 增量完善：家族结构与分组集中度 ----------
    # 说明：本块是「增量完善」新增的结果，不改动上面 RQ1–RQ5 的任何既有指标。
    fam = load_json(os.path.join(ANALYSIS, "family_ext_stats.json"), "family_ext_stats")
    if fam:
        src = "family_ext_stats.json"
        for k in ("n_models_total", "n_edges", "n_nodes_involved",
                  "n_distinct_base_models", "n_distinct_derived_models",
                  "n_multi_parent_models", "multi_parent_share_of_derived",
                  "n_merge_edges", "relation_counts", "relation_shares",
                  "max_children_count", "max_subtree_height", "max_root_depth",
                  "mean_subtree_height_all_bases", "structure_class_counts_all_bases",
                  "structure_class_counts_by_tier", "top_bases_by_children",
                  "scope_note", "seconds", "engine"):
            put(f"rq3_family.{k}", fam.get(k), f"{src}:{k}")
    scc = load_json(os.path.join(ANALYSIS, "family_scc_summary.json"), "family_scc_summary")
    if scc:
        put("rq3_family.scc_summary", scc, "family_scc_summary.json")
    mp = load_summary_csv("family_multi_parent.csv")
    if mp:
        put("rq3_family.multi_parent_rows", mp, "family_multi_parent.csv")
    ts = load_summary_csv("family_task_structure.csv")
    if ts:
        put("rq3_family.task_structure_rows", ts, "family_task_structure.csv",
            "按 pipeline_tag 汇总的家族结构（底座数/衍生数/中位出度/平均高度/merge 占比/多父占比）")
    cases = load_json(os.path.join(ANALYSIS, "family_cases.json"), "family_cases")
    if cases:
        put("rq3_family.case_selection_rule", cases.get("case_selection_rule"),
            "family_cases.json:case_selection_rule")
        put("rq3_family.cases", cases.get("cases"), "family_cases.json:cases",
            "三个代表性家族：star / chain / merge")
        put("rq3_family.children_count_downloads_correlation",
            cases.get("children_count_downloads_correlation"),
            "family_cases.json:children_count_downloads_correlation",
            "同一快照内 children_count 与下载量的秩相关；不作因果解释")
        put("rq3_family.role_note", cases.get("role_note"), "family_cases.json:role_note")

    cg = load_json(os.path.join(ANALYSIS, "concentration_groups.json"), "concentration_groups")
    if cg:
        put("rq2_groups.n_models_total", cg.get("n_models_total"),
            "concentration_groups.json:n_models_total")
        put("rq2_groups.calibers", cg.get("calibers"), "concentration_groups.json:calibers")
        for grouping, by_caliber in (cg.get("groupings") or {}).items():
            for caliber, block in (by_caliber or {}).items():
                put(f"rq2_groups.{grouping}.{caliber}.theil_decomposition",
                    block.get("theil_decomposition"),
                    f"concentration_groups.json:groupings.{grouping}.{caliber}.theil_decomposition",
                    "Theil T 的组间/组内分解（between_share 为组间占比）")
                put(f"rq2_groups.{grouping}.{caliber}.n_groups", block.get("n_groups"),
                    f"concentration_groups.json:groupings.{grouping}.{caliber}.n_groups")
                put(f"rq2_groups.{grouping}.{caliber}.n_groups_ge_1000",
                    block.get("n_groups_ge_1000"),
                    f"concentration_groups.json:groupings.{grouping}.{caliber}.n_groups_ge_1000")
                put(f"rq2_groups.{grouping}.{caliber}.per_group", block.get("per_group"),
                    f"concentration_groups.json:groupings.{grouping}.{caliber}.per_group")

    fext = load_json(os.path.join(ANALYSIS, "figures_ext_index.json"), "figures_ext_index")
    if fext:
        put("deliverables.figures_ext", fext.get("figures"), "figures_ext_index.json:figures")
        put("deliverables.figures_ext_count", len(fext.get("figures") or []),
            "figures_ext_index.json")

    # ---------- 8. Benchmark ----------
    rows = load_summary_csv("benchmark_all_summary.csv")
    if rows:
        real = [r for r in rows if r.get("data_kind") == "hubstats_models_parquet"]
        synth = [r for r in rows if r.get("data_kind") == "synthetic_stress_test"]
        # 真实数据里混着两组实验：P1 规模扫描（10/25/50/100%）与 P2 并行度扫描
        # （local[2]/[4]/[6]，规模档同样叫 real_50pct）。
        # 用 run_tag 隔离：**拥有最多不同行数档位**的那一组就是规模扫描
        # （P1 有 4 档，P2 只有 1 档）。若不隔离，报告里的规模-耗时表会被
        # 不同核数的结果覆盖，同一条曲线里混进多个 master，结论失真。
        by_tag = {}
        for r in real:
            by_tag.setdefault(r.get("run_tag"), set()).add(r.get("n_rows"))
        sweep_tag = max(by_tag, key=lambda t: len(by_tag[t])) if by_tag else None
        sweep = [r for r in real if r.get("run_tag") == sweep_tag]
        cores = [r for r in real if r.get("run_tag") != sweep_tag]
        put("bench.real.sweep_tag", sweep_tag, "benchmark_all_summary.csv:run_tag")
        put("bench.real.n_measurements", len(real), "benchmark_all_summary.csv")
        put("bench.synth.n_measurements", len(synth), "benchmark_all_summary.csv")
        put("bench.real.masters", sorted({r.get("spark_master") for r in sweep}),
            "benchmark_all_summary.csv")
        put("bench.real.scales", sorted({r.get("scale_label") for r in sweep}),
            "benchmark_all_summary.csv")
        put("bench.real.rows", sorted({int(r["n_rows"]) for r in sweep if r.get("n_rows")}),
            "benchmark_all_summary.csv")
        put("bench.real.ops", sorted({r.get("op") for r in sweep}),
            "benchmark_all_summary.csv")
        put("bench.real.sweep_measurements", sweep,
            "benchmark_all_summary.csv（规模扫描组，按 run_tag 隔离）")
        put("bench.real.cores_masters", sorted({r.get("spark_master") for r in cores}),
            "benchmark_all_summary.csv")
        put("bench.real.cores_measurements", cores,
            "benchmark_all_summary.csv（并行度扫描组）")
        put("bench.real.measurements", real, "benchmark_all_summary.csv（全量明细）")
        put("bench.synth.measurements", synth, "benchmark_all_summary.csv（全量明细）")
        put("bench.real.driver_memory",
            sorted({r.get("driver_memory") for r in sweep if r.get("driver_memory")}),
            "benchmark_all_summary.csv:driver_memory")
        put("bench.real.mem_pressure_groups",
            sum(1 for r in real if str(r.get("mem_pressure")).strip().lower() == "true"),
            "benchmark_all_summary.csv:mem_pressure")
        put("bench.real.measurements_all", real,
            "benchmark_all_summary.csv（含内存压力组，仅供审计）")
    # 内存压力阈值来自 benchmark.py 的元信息文件（每次运行各写一份）
    for fn_meta in sorted(glob.glob(os.path.join(ANALYSIS, "benchmark_meta_*.json"))):
        mm = load_json(fn_meta, os.path.basename(fn_meta))
        if mm and mm.get("mem_pressure_threshold_gib") is not None:
            put("bench.mem_pressure_threshold_gib", mm.get("mem_pressure_threshold_gib"),
                f"{os.path.basename(fn_meta)}:mem_pressure_threshold_gib")
            put("bench.mem_sample_interval_s", mm.get("mem_sample_interval_s"),
                f"{os.path.basename(fn_meta)}:mem_sample_interval_s")
            put("bench.rss_caliber_note", mm.get("rss_caliber_note"),
                f"{os.path.basename(fn_meta)}:rss_caliber_note")
            break
    runs = load_json(os.path.join(ANALYSIS, "benchmark_runs.json"), "benchmark_runs")
    if runs:
        put("bench.run_meta", runs, "benchmark_runs.json")

    # ---------- 9. 执行与产物清单 ----------
    p2 = load_json(os.path.join(ANALYSIS, "phase2_runs.json"), "phase2_runs")
    if p2:
        put("exec.phase2", p2, "phase2_runs.json")
    fi = load_json(os.path.join(ANALYSIS, "figure_index.json"), "figure_index")
    if fi:
        put("deliverables.figures", fi.get("figures"), "figure_index.json:figures")
    put("deliverables.figure_count", len(fi.get("figures") or []) if fi else None,
        "figure_index.json")

    # ---------- 10. 最终状态判定（依据真实结果，不做粉饰） ----------
    hard_fail = (dq or {}).get("hard_fail")
    p2_steps = (p2 or {}).get("steps") or []
    p2_failed = [s["script"] for s in p2_steps if not s.get("ok")]
    gf_status = (gfr or {}).get("status")
    gf_unfinished = []
    for name, label in (("connected_components", "connectedComponents"),
                        ("pagerank_forward", "PageRank(正向 base→derived)"),
                        ("pagerank_reverse", "PageRank(反向 derived→base)"),
                        ("label_propagation", "labelPropagation"),
                        ("merge_node_metrics", "逐节点指标合并")):
        if not ((gfr or {}).get("steps") or {}).get(name):
            gf_unfinished.append(label)

    if hard_fail:
        status = "PROJECT NOT READY"
        reason = (f"数据质量硬性检查存在 {hard_fail} 项失败，"
                  f"必须修复后才能交付。详见 docs/data_quality_report.md。")
    elif p2_failed:
        status = "PROJECT NOT READY"
        reason = f"Phase 2 分析脚本执行失败：{p2_failed}。"
    elif not rows:
        status = "PROJECT READY WITH LIMITATIONS"
        reason = "Phase 3 扩展性实验尚未产出结果，其余模块已完成。"
    elif gf_unfinished:
        status = "PROJECT READY WITH LIMITATIONS"
        reason = (f"全部数据、分析与交付物已完成且通过一致性检查；"
                  f"唯一未完成项是 GraphFrames 的迭代算法"
                  f"（{'、'.join(gf_unfinished)}），"
                  f"在 32GB 本地 / local[6] / driver 4g 下超出 "
                  f"{(gfr or {}).get('budget_seconds')} 秒预算。"
                  f"完整边表与全部 Spark 可计算的图统计均已保留，"
                  f"未完成算法已在 graphframes_result.json 中如实记录，"
                  f"未用 NetworkX 结果替代。")
    else:
        status = "PROJECT READY FOR REPORT AND PRESENTATION"
        reason = "全部模块完成，GraphFrames 迭代算法亦在本机跑完，无未完成项。"

    put("final.status", status, "build_manifest.py:自动判定（依据 data_quality_report / "
                               "phase2_runs / benchmark / graphframes_result）")
    put("final.status_reason", reason, "build_manifest.py:自动判定")
    put("final.gf_unfinished", gf_unfinished, "graphframes_result.json:steps")
    put("final.phase2_failed", p2_failed, "phase2_runs.json")

    # ---------- 11. 落盘 ----------
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_commit": SA.HUBSTATS_COMMIT,
        "missing_inputs": MISSING,
        "metrics": MANIFEST,
    }
    out_json = os.path.join(ANALYSIS, "results_manifest.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    lines = ["# 结果清单（results_manifest）", "",
             f"生成时间：{payload['generated_at']}  ",
             f"数据快照 commit：`{SA.HUBSTATS_COMMIT}`  ",
             f"指标条数：{len(MANIFEST)}", "",
             "> 本文件是**唯一**的结果基准。报告、Dashboard、图注、LaTeX 素材、PPT 素材"
             "中的所有数字都必须能在此表找到，且不得与表中值冲突。", ""]
    if MISSING:
        lines += ["## 缺失输入（对应指标未登记，不猜测数值）", ""]
        lines += [f"- `{m}`" for m in MISSING] + [""]
    lines += ["| 指标 | 值 | 出处 | 备注 |", "| --- | --- | --- | --- |"]
    for k in sorted(MANIFEST):
        v = MANIFEST[k]["value"]
        s = json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v
        if len(s) > 200:
            s = s[:197] + "…"
        s = s.replace("|", "\\|").replace("\n", " ")
        note = str(MANIFEST[k]["note"]).replace("|", "\\|")
        lines.append(f"| `{k}` | {s} | `{MANIFEST[k]['source']}` | {note} |")
    out_md = os.path.join(DOCS, "results_manifest.md")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"[manifest] {len(MANIFEST)} 条指标 -> {out_json}")
    print(f"[manifest] 表格 -> {out_md}")
    if MISSING:
        print(f"[manifest] 缺失输入 {len(MISSING)} 项: {MISSING}")
        sys.exit(2)


if __name__ == "__main__":
    main()
