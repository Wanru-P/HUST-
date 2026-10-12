# -*- coding: utf-8 -*-
"""Part C · RQ4 补充：likes→downloads 关联的「陈旧性」对照。

假设：likes（累计）与 downloads（近 30 天）的关联，在「长期未更新（陈旧）」的模型上会衰减——
陈旧模型的 likes 反映历史热度、downloads 反映当前热度，二者会脱钩。
用 lastModified 距今的天数把模型分层，看 likes_log 与 log1p(downloads) 的关联强度是否随陈旧度递减。

用法：
    HF_BRONZE_PATH=/path/to/models.parquet python supplementary/staleness_analysis.py
输出：supplementary/staleness_result.json（含每层样本量、零下载占比、Pearson/Spearman）
"""
import json
import os

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET = os.environ.get(
    "HF_BRONZE_PATH",
    os.path.join(PROJECT_ROOT, "data_hubstats", "bronze", "models.parquet"))
OUT_PATH = os.path.join(PROJECT_ROOT, "supplementary", "staleness_result.json")
SNAPSHOT_DATE = pd.Timestamp("2026-10-11", tz="UTC")

BINS = [
    (0, 30, "fresh (≤30d)"),
    (31, 180, "recent (31–180d)"),
    (181, 365, "mid (181–365d)"),
    (366, None, "stale (>365d)"),
]


def main():
    print("[1/3] 读 parquet ...", flush=True)
    raw = pd.read_parquet(PARQUET, columns=["likes", "downloads", "lastModified"])
    df = pd.DataFrame({
        "likes": pd.to_numeric(raw["likes"], errors="coerce").fillna(0),
        "downloads": pd.to_numeric(raw["downloads"], errors="coerce").fillna(0),
        "last_modified": pd.to_datetime(raw["lastModified"], utc=True, errors="coerce"),
    })
    df["staleness_days"] = (SNAPSHOT_DATE - df["last_modified"]).dt.days
    df["likes_log"] = np.log1p(df["likes"])
    df["label"] = np.log1p(df["downloads"])
    df = df.dropna(subset=["staleness_days"])
    print(f"      有效模型 = {len(df):,}", flush=True)

    print("[2/3] 陈旧度分布（days since lastModified）...", flush=True)
    q = df["staleness_days"].quantile([0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0])
    for k, v in q.items():
        print(f"      q{k:.2f} = {v:,.0f} 天", flush=True)

    print("[3/3] 分层关联强度 ...", flush=True)
    rows = []
    for lo, hi, name in BINS:
        mask = (df["staleness_days"] >= lo)
        if hi is not None:
            mask &= (df["staleness_days"] <= hi)
        sub = df[mask]
        if len(sub) == 0:
            continue
        pearson = float(sub["likes_log"].corr(sub["label"], method="pearson"))
        spearman = float(sub["likes_log"].corr(sub["label"], method="spearman"))
        zero_share = float((sub["downloads"] == 0).mean())
        pos = sub[sub["downloads"] > 0]
        pearson_pos = float(pos["likes_log"].corr(pos["label"], method="pearson")) if len(pos) > 1 else None
        rows.append({
            "group": name,
            "n": int(len(sub)),
            "zero_share": round(zero_share, 4),
            "pearson_likes_downloads": round(pearson, 4),
            "spearman_likes_downloads": round(spearman, 4),
            "pearson_positive_only": round(pearson_pos, 4) if pearson_pos is not None else None,
        })
        print(f"  {name:<18} n={len(sub):>9,}  zero={zero_share:>6.1%}  "
              f"pearson={pearson:+.4f}  spearman={spearman:+.4f}  "
              f"pearson(>0)={pearson_pos if pearson_pos is None else round(pearson_pos,4):>+7}",
              flush=True)

    out = {"snapshot": str(SNAPSHOT_DATE.date()), "bins": rows}
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n-> {OUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
