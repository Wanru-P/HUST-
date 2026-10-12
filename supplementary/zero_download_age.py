# -*- coding: utf-8 -*-
"""Part A 补充 · 零下载模型的年龄构成（RQ2 长尾稳健性）。

问题：43.54% 的零下载模型里，多少是「刚发布、30 天下载窗口尚未走完」的时间伪影，
多少是「创建已超过 30 天、在当前统计窗口内没有下载记录」的部分？
注：downloads 为「近 30 天」口径，创建不满 30 天的模型其下载窗口被截断，其「零下载」不具可比性。
     对创建超过 30 天的模型，本脚本只说「当前窗口内无下载记录」，不推断其「无人问津」。

用法：
    HF_BRONZE_PATH=/path/to/models.parquet python supplementary/zero_download_age.py
输出：supplementary/zero_download_age_result.json
"""
import json
import os

import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET = os.environ.get(
    "HF_BRONZE_PATH",
    os.path.join(PROJECT_ROOT, "data_hubstats", "bronze", "models.parquet"))
OUT_PATH = os.path.join(PROJECT_ROOT, "supplementary", "zero_download_age_result.json")
SNAPSHOT_DATE = pd.Timestamp("2026-10-11", tz="UTC")

BUCKETS = [
    (0, 30, "≤30天"), (31, 90, "31–90天"), (91, 180, "91–180天"),
    (181, 365, "181–365天"), (366, 730, "1–2年"), (731, None, ">2年"),
]


def main():
    print("[1/3] 读 parquet ...", flush=True)
    raw = pd.read_parquet(PARQUET, columns=["id", "downloads", "createdAt"])
    df = pd.DataFrame({
        "downloads": pd.to_numeric(raw["downloads"], errors="coerce").fillna(0),
        "created": pd.to_datetime(raw["createdAt"], utc=True, errors="coerce"),
    })
    df["age_days"] = (SNAPSHOT_DATE - df["created"]).dt.days
    df = df.dropna(subset=["age_days"])
    n_total = len(df)
    n_zero = int((df["downloads"] == 0).sum())
    print(f"      总模型 = {n_total:,}  零下载 = {n_zero:,}（{n_zero / n_total:.2%}）", flush=True)

    print("[2/3] 按年龄桶统计 ...", flush=True)
    rows = []
    for lo, hi, name in BUCKETS:
        m = (df["age_days"] >= lo) & ((df["age_days"] <= hi) if hi is not None else True)
        sub = df[m]
        nz = int((sub["downloads"] == 0).sum())
        rows.append({
            "bucket": name, "n_total": int(len(sub)), "n_zero": nz,
            "zero_rate": round(nz / len(sub), 4),
            "share_of_all_zeros": round(nz / n_zero, 4),
        })
        print(f"  {name:<10} 总数={len(sub):>9,}  零下载={nz:>9,}  "
              f"零率={nz/len(sub):>6.2%}  占全部零的 {nz/n_zero:>6.2%}", flush=True)

    print("[3/3] 关键拆解（≤30 天 = 窗口截断）...", flush=True)
    young = df[df["age_days"] <= 30]
    old = df[df["age_days"] > 30]
    nz_young = int((young["downloads"] == 0).sum())
    nz_old = int((old["downloads"] == 0).sum())
    refined_rate = nz_old / len(old)
    print(f"  创建 ≤30 天（窗口截断）的零下载 = {nz_young:,}（占全部零的 {nz_young/n_zero:.2%}）", flush=True)
    print(f"  创建 >30 天（窗口完整）的零下载 = {nz_old:,}（占全部零的 {nz_old/n_zero:.2%}）", flush=True)
    print(f"  仅计创建 >30 天（窗口完整）时的零下载率 = {refined_rate:.2%}（原始口径 43.54%）", flush=True)

    out = {
        "snapshot": str(SNAPSHOT_DATE.date()),
        "n_total": int(n_total),
        "n_zero": int(n_zero),
        "zero_rate_overall": round(n_zero / n_total, 4),
        "buckets": rows,
        "n_zero_age_le30": int(nz_young),
        "n_zero_age_gt30": int(nz_old),
        "share_zero_le30_of_all_zeros": round(nz_young / n_zero, 4),
        "share_zero_gt30_of_all_zeros": round(nz_old / n_zero, 4),
        "refined_zero_rate_excl_le30": round(refined_rate, 4),
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n-> {OUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
