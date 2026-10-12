# -*- coding: utf-8 -*-
"""Part E · 数据质量补充：负维护跨度（lastModified < createdAt）量化。

背景：维护跨度 maintenance_span_days = lastModified − createdAt，理论应 ≥ 0。
但 RQ5 任务汇总中 multiple-choice 任务的 avg_maintenance_span_days = −2.35，
说明存在 lastModified 早于 createdAt 的记录（物理上不可能），属时间戳口径/记录问题。
本脚本量化：负跨度记录的规模、占比、幅度分布，以及是否集中在特定任务/库。
只量化、不臆断根因（与报告对 RQ1 2022-03 时间戳异常的口径一致）。

用法：
    HF_BRONZE_PATH=/path/to/models.parquet python supplementary/maintenance_span_sanity.py
输出：supplementary/maintenance_span_sanity_result.json
依赖：pandas / numpy / pyarrow
"""
import json
import os

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET = os.environ.get(
    "HF_BRONZE_PATH",
    os.path.join(PROJECT_ROOT, "data_hubstats", "bronze", "models.parquet"))
# 输出目录可用 SUPP_OUT_DIR 覆盖（沙箱/CI 下仓库子目录不可写时使用）
OUT_DIR = os.environ.get("SUPP_OUT_DIR", os.path.join(PROJECT_ROOT, "supplementary"))
OUT_PATH = os.path.join(OUT_DIR, "maintenance_span_sanity_result.json")

BUCKETS = [
    (None, -365, "≤−365天"),
    (-364, -31, "−364~−31天"),
    (-30, -8, "−30~−8天"),
    (-7, -1, "−7~−1天"),
    (0, 0, "0（0≤Δ<1天）"),
    (1, 7, "1~7天"),
    (8, 30, "8~30天"),
    (31, 365, "31~365天"),
    (366, None, ">365天"),
]


def _bucket_mask(span, lo, hi):
    if lo is None:
        return span <= hi
    if hi is None:
        return span >= lo
    return (span >= lo) & (span <= hi)


def _group_negative(table, key):
    g = table.groupby(key)
    df = pd.DataFrame({
        "n_models": g.size(),
        "n_negative_span": g["span_days"].apply(lambda s: (s < 0).sum()).astype(int),
        "mean_span_days": g["span_days"].mean(),
    }).reset_index()
    df["negative_rate"] = (df["n_negative_span"] / df["n_models"]).round(6)
    df["mean_span_days"] = df["mean_span_days"].round(4)
    return df


def main():
    print("[1/3] 读 parquet ...", flush=True)
    raw = pd.read_parquet(PARQUET, columns=["createdAt", "lastModified",
                                            "pipeline_tag", "library_name"])
    df = pd.DataFrame({
        "created": pd.to_datetime(raw["createdAt"], utc=True, errors="coerce"),
        "modified": pd.to_datetime(raw["lastModified"], utc=True, errors="coerce"),
        "task": raw["pipeline_tag"].fillna("unknown"),
        "lib": raw["library_name"].fillna("unknown"),
    })
    n_total = len(df)
    valid = df.dropna(subset=["created", "modified"]).copy()
    # ⚠️ .dt.days 向下取整：span_days==0 表示 0 ≤ Δ < 1 天，**不等于**「同一日历日期」。
    #    因此同时给出精确到秒的 Δ==0 计数，避免把「整日法为 0」与「日历同日」混为一谈。
    valid["span_td"] = valid["modified"] - valid["created"]
    valid["span_days"] = valid["span_td"].dt.days
    n_valid = len(valid)
    n_neg = int((valid["span_days"] < 0).sum())
    n_zero = int((valid["span_days"] == 0).sum())
    n_pos = int((valid["span_days"] > 0).sum())
    n_exact_zero = int((valid["span_td"] == pd.Timedelta(0)).sum())
    print(f"      总行 = {n_total:,}  时间戳齐全 = {n_valid:,}", flush=True)
    print(f"      负跨度 = {n_neg:,}（{n_neg/n_valid:.3%}）  整日法0 = {n_zero:,}（其中精确 Δ==0 为 {n_exact_zero:,}）  正 = {n_pos:,}", flush=True)

    print("[2/3] 幅度分布 ...", flush=True)
    rows = []
    for lo, hi, name in BUCKETS:
        sub = valid[_bucket_mask(valid["span_days"], lo, hi)]
        rows.append({"bucket": name, "n": int(len(sub)), "share": round(len(sub) / n_valid, 6)})
        print(f"  {name:<12} n={len(sub):>9,}  ({len(sub)/n_valid:>6.3%})", flush=True)

    print("[3/3] 是否集中在特定任务/库 ...", flush=True)
    top_tasks = _group_negative(valid, "task").sort_values(
        "n_negative_span", ascending=False).head(12).to_dict("records")
    top_libs = _group_negative(valid, "lib").sort_values(
        "n_negative_span", ascending=False).head(10).to_dict("records")

    out = {
        "n_total": int(n_total),
        "n_valid_timestamps": int(n_valid),
        "n_negative_span": int(n_neg),
        "negative_share": round(n_neg / n_valid, 6),
        "n_zero_span": int(n_zero),
        "n_exact_zero_span": int(n_exact_zero),
        "zero_span_definition": "span_days==0 表示 0 ≤ (lastModified − createdAt) < 1 天（.dt.days 向下取整），不等于日历日期相同；n_exact_zero_span 为精确 Δ==0 的计数",
        "zero_share": round(n_zero / n_valid, 6),
        "n_positive_span": int(n_pos),
        "min_span_days": int(valid["span_days"].min()),
        "max_span_days": int(valid["span_days"].max()),
        "mean_span_days": round(float(valid["span_days"].mean()), 4),
        "median_span_days": int(valid["span_days"].median()),
        "distribution_buckets": rows,
        "top_tasks_by_negative_span": top_tasks,
        "top_libraries_by_negative_span": top_libs,
        "note": "负跨度 lastModified < createdAt 物理上不可能，属时间戳口径/记录问题。本脚本只量化、不臆断根因（与 RQ1 2022-03 异常同口径处理）。",
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n-> {OUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
