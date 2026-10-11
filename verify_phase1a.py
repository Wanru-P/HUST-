#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_phase1a.py — Phase 1A 端到端验收检查（全部真实读盘，不产生任何伪造数字）。

检查项：
  1. Silver/Gold 各表行数
  2. 关键字段缺失率（downloads / downloadsAllTime / likes / base_models / created_at）
  3. 去重（model_id 唯一）
  4. Tags 拆表、Base Model Edges 构建
  5. 时间字段转换（created_at 可解析为 timestamp，范围合理）
  6. Parquet 写入 + 回读一致
  7. Spark 引擎 vs pandas 引擎结果一致（行数 / 下载总量 / Gini）
  8. 核心统计可复现性（Gini / Top-1% 份额）

用法：
    python verify_phase1a.py [--root <data_hubstats>]
"""
import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

import schema_adapter as SA


def read_table(path):
    """读一个 parquet 目录（Spark part-*.parquet 或 pandas part-00000.parquet）。"""
    if not os.path.isdir(path):
        return None
    return pd.read_parquet(path)


def missing_rate(df, col):
    if df is None or col not in df.columns:
        return None
    return round(float(df[col].isna().mean()), 6)


def gini(x):
    x = np.sort(np.asarray(x, dtype=float))
    n = len(x)
    s = x.sum()
    if n == 0 or s == 0:
        return 0.0
    return float((n + 1 - 2.0 * (np.cumsum(x) / s).sum()) / n)


def describe_engine(root, subdir):
    """对某引擎（spark=subdir 前缀为空，local=_local）的产物做检查。"""
    silver = os.path.join(root, "silver" + subdir)
    gold = os.path.join(root, "gold" + subdir)
    models = read_table(os.path.join(silver, "models"))
    tags = read_table(os.path.join(silver, "model_tags"))
    edges = read_table(os.path.join(silver, "model_base_edges"))
    enriched = read_table(os.path.join(gold, "models_enriched"))

    info = {"silver_dir": silver, "gold_dir": gold}
    if models is not None:
        info["n_models"] = int(len(models))
        info["n_unique_model_id"] = int(models["model_id"].nunique())
        info["dup_model_id"] = int(len(models) - models["model_id"].nunique())
        info["missing_rate"] = {c: missing_rate(models, c) for c in
                                ["author", "created_at", "last_modified", "downloads",
                                 "downloadsAllTime", "likes", "pipeline_tag",
                                 "library_name", "tags", "gated", "base_models"]}
        info["downloads_sum"] = int(pd.to_numeric(models["downloads"], errors="coerce").fillna(0).sum())
        info["downloadsAllTime_sum"] = int(pd.to_numeric(models["downloadsAllTime"], errors="coerce").fillna(0).sum())
        info["downloads_zero"] = int((pd.to_numeric(models["downloads"], errors="coerce").fillna(0) == 0).sum())
        ts = pd.to_datetime(models["created_at"], utc=True, errors="coerce")
        info["created_at_parse_fail"] = int(ts.isna().sum())
        info["created_at_iso_z_ok"] = bool(
            models["created_at"].astype(str).str.match(
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$").all())
        info["created_at_sample"] = list(models["created_at"].astype(str).head(2))
        info["created_at_min"] = str(ts.min())
        info["created_at_max"] = str(ts.max())
        info["gated_all_null"] = bool(models["gated"].isna().all())
        if "base_relation" in models.columns:
            info["base_relation_counts"] = models["base_relation"].value_counts(dropna=False).head(8).to_dict()
        info["gini_downloads"] = round(gini(pd.to_numeric(models["downloads"], errors="coerce").fillna(0)), 6)
        info["gini_downloadsAllTime"] = round(gini(pd.to_numeric(models["downloadsAllTime"], errors="coerce").fillna(0)), 6)
        dl = pd.to_numeric(models["downloads"], errors="coerce").fillna(0).sort_values(ascending=False).to_numpy()
        tot = dl.sum() or 1
        info["top1_pct_share"] = round(float(dl[:max(1, len(dl)//100)].sum() / tot), 6)
    if tags is not None:
        info["n_tag_rows"] = int(len(tags))
        info["n_unique_tags"] = int(tags["tag"].nunique())
    if edges is not None:
        info["n_edges"] = int(len(edges))
        info["n_edge_src"] = int(edges["src"].nunique())
        info["edge_relation_counts"] = edges["relation"].value_counts().head(8).to_dict()
    if enriched is not None:
        info["gold_models_enriched_rows"] = int(len(enriched))
        info["family_size_ge1"] = bool((enriched["family_size"] >= 1).all())
        info["children_count_sum"] = int(enriched["children_count"].sum())
        info["enriched_has_month_num"] = bool("month_num" in enriched.columns)

    gold_rows = {}
    for name in SA.GOLD_TABLES + SA.GOLD_EXTRA_TABLES:
        table = read_table(os.path.join(gold, name))
        gold_rows[name] = None if table is None else int(len(table))
    info["gold_rows"] = gold_rows

    lorenz = read_table(os.path.join(gold, "lorenz_points"))
    if lorenz is not None:
        info["lorenz_metrics"] = sorted(lorenz["metric"].unique().tolist())
        info["lorenz_points"] = {m: int((lorenz["metric"] == m).sum())
                                 for m in info["lorenz_metrics"]}
    cpath = os.path.join(gold, "concentration_stats.json")
    if os.path.exists(cpath):
        with open(cpath, encoding="utf-8") as f:
            cs = json.load(f)
        info["concentration_gini"] = {"downloads": cs["downloads"]["gini"],
                                      "downloadsAllTime": cs["downloadsAllTime"]["gini"]}
        info["concentration_top1"] = {"downloads": cs["downloads"]["top_1pct_share"],
                                      "downloadsAllTime": cs["downloadsAllTime"]["top_1pct_share"]}

    task = read_table(os.path.join(gold, "task_monthly_stats"))
    if task is not None and "month" in task.columns:
        months = task["month"].astype(str)
        info["task_month_is_yyyy_mm"] = bool(months.str.match(r"^\d{4}-\d{2}$").all())
        info["task_month_sample"] = sorted(months.unique())[:3]
    return info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=None)
    args = ap.parse_args()
    root = args.root or SA.data_root()

    report = {"root": root}
    report["spark"] = describe_engine(root, "")
    report["local"] = describe_engine(root, "_local")

    # 引擎一致性
    s, l = report["spark"], report["local"]
    checks = {}
    for key in ["n_models", "n_tag_rows", "n_edges", "downloads_sum", "downloadsAllTime_sum"]:
        if key in s and key in l:
            checks[key] = {"spark": s[key], "local": l[key], "match": s[key] == l[key]}
    if "gini_downloads" in s and "gini_downloads" in l:
        checks["gini_downloads"] = {"spark": s["gini_downloads"], "local": l["gini_downloads"],
                                    "abs_diff": abs(s["gini_downloads"] - l["gini_downloads"])}
    for name in SA.GOLD_TABLES + SA.GOLD_EXTRA_TABLES:
        a = s.get("gold_rows", {}).get(name)
        b = l.get("gold_rows", {}).get(name)
        if a is not None and b is not None:
            checks["gold." + name] = {"spark": a, "local": b, "match": a == b}
    if "concentration_gini" in s and "concentration_gini" in l:
        for metric in ("downloads", "downloadsAllTime"):
            checks[f"gini.{metric}"] = {
                "spark": s["concentration_gini"][metric],
                "local": l["concentration_gini"][metric],
                "abs_diff": abs(s["concentration_gini"][metric]
                                - l["concentration_gini"][metric])}
            a = s["concentration_top1"][metric]
            b = l["concentration_top1"][metric]
            # total == 0（如旧 Demo 无 downloadsAllTime）时份额为 None，两引擎应同时为 None
            checks[f"top1.{metric}"] = {
                "spark": a, "local": b,
                "match": (a is None and b is None) or a == b,
                "abs_diff": None if (a is None or b is None) else abs(a - b)}
    report["engine_consistency"] = checks

    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    out = os.path.join(root, "phase1a_verify_report.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[verify] 报告 -> {out}")

    hard_fail = []
    if s.get("n_models") and s.get("dup_model_id"):
        hard_fail.append("spark silver/models 存在重复 model_id")
    if s.get("gated_all_null") is False:
        hard_fail.append("gated 未保持全 NULL")
    if s.get("created_at_parse_fail", 0) > 0:
        hard_fail.append("created_at 存在无法解析的记录")
    if s.get("created_at_iso_z_ok") is False:
        hard_fail.append("spark silver/models.created_at 不是 ISO-8601 UTC（缺 Z / 空格分隔）")
    if l.get("created_at_iso_z_ok") is False:
        hard_fail.append("local silver/models.created_at 不是 ISO-8601 UTC")
    if s.get("task_month_is_yyyy_mm") is False:
        hard_fail.append("task_monthly_stats.month 不是 yyyy-MM 字符串（疑似按整型月份分组）")
    if s.get("enriched_has_month_num") is False:
        hard_fail.append("models_enriched 缺少 month_num 列（与旧 Demo 契约不一致）")
    if s.get("lorenz_metrics") is not None and \
            sorted(s["lorenz_metrics"]) != ["downloads", "downloadsAllTime"]:
        hard_fail.append("lorenz_points.metric 必须恰好是 downloads / downloadsAllTime 两个口径")
    for k, v in checks.items():
        if isinstance(v, dict) and v.get("match") is False:
            hard_fail.append(f"引擎不一致: {k}")
        if isinstance(v, dict) and v.get("abs_diff") is not None and v["abs_diff"] > 1e-9:
            hard_fail.append(f"引擎数值不一致: {k} (abs_diff={v['abs_diff']})")
    if hard_fail:
        print("[FAIL] " + "; ".join(hard_fail), file=sys.stderr)
        sys.exit(1)
    print("[OK] Phase 1A 验收检查通过")


if __name__ == "__main__":
    main()
