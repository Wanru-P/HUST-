#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_distribution.py — Phase 1B：下载量集中度「新频数聚合口径」与「旧全局窗口口径」对拍。

背景：Phase 1B 把 spark_etl.py 的 download_distribution 从
      「无 partitionBy 的全局窗口排序」改成「频数聚合 + 组内序号」。
      这是**语义等价的替换**，必须用真实数据逐行证明，不能靠"看起来一样"。

本脚本在 50,218 条真实样本上验证 5 件事：
  1. 逐模型 rank 完全一致
  2. 逐模型 cum_model_pct 完全一致
  3. 逐模型 cum_download_pct 完全一致（浮点误差内）
  4. 频数加权 Gini == 逐模型秩式 Gini == 逐模型累计式 Gini
  5. Top-1/5/10% 份额一致（并列值不影响结果）

运行：python test_distribution.py [--bronze <path>]
输出：data_hubstats/_smoke/test_distribution_result.json
"""
import argparse
import json
import os

import numpy as np
import pandas as pd

import schema_adapter as SA
import stats_utils as SU

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BRONZE = os.path.join(PROJECT_ROOT, "data_hubstats", "bronze_sample", "models.parquet")
OUT = os.path.join(PROJECT_ROOT, "data_hubstats", "_smoke", "test_distribution_result.json")


def old_style(models):
    """旧口径：按 (downloads, model_id) 全局升序排序后逐行累计。"""
    dl = models[["model_id", "downloads"]].sort_values(
        ["downloads", "model_id"], kind="mergesort").reset_index(drop=True)
    n = len(dl)
    total = float(dl["downloads"].sum())
    dl["rank"] = np.arange(1, n + 1)
    dl["cum_model_pct"] = dl["rank"] / n
    dl["cum_download_pct"] = dl["downloads"].cumsum() / (total if total else 1.0)
    return dl, n, total


def new_style(models):
    """新口径：频数聚合 -> 按取值查起始秩 + 组内按 model_id 排序的序号。"""
    n = len(models)
    total = float(models["downloads"].sum())
    freq = (models.groupby("downloads").size().rename("freq").reset_index()
            .sort_values("downloads").reset_index(drop=True))
    freq["cum_freq"] = freq["freq"].cumsum()
    freq["cum_value"] = (freq["downloads"] * freq["freq"]).cumsum()
    freq["start_rank"] = freq["cum_freq"] - freq["freq"] + 1
    freq["prev_cum_value"] = freq["cum_value"] - freq["downloads"] * freq["freq"]

    dl = models[["model_id", "downloads"]].merge(
        freq[["downloads", "start_rank", "prev_cum_value"]], on="downloads", how="left")
    dl = dl.sort_values(["downloads", "model_id"], kind="mergesort").reset_index(drop=True)
    dl["_off"] = dl.groupby("downloads").cumcount()
    dl["rank"] = dl["start_rank"] + dl["_off"]
    dl["cum_model_pct"] = dl["rank"] / n
    dl["cum_download_pct"] = (dl["prev_cum_value"] + (dl["_off"] + 1) * dl["downloads"]) \
        / (total if total else 1.0)
    return dl, freq, n, total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", default=DEFAULT_BRONZE)
    args = ap.parse_args()

    raw = pd.read_parquet(args.bronze, columns=["id", "downloads", "downloadsAllTime"])
    models = raw.rename(columns={"id": "model_id"})
    models["downloads"] = models["downloads"].astype("int64")
    models["downloadsAllTime"] = models["downloadsAllTime"].astype("int64")

    old, n, total = old_style(models)
    new, freq, n2, total2 = new_style(models)
    old = old.sort_values(["downloads", "model_id"], kind="mergesort").reset_index(drop=True)
    new = new.sort_values(["downloads", "model_id"], kind="mergesort").reset_index(drop=True)

    result = {
        "bronze": os.path.basename(args.bronze),
        "n_models": int(n),
        "total_downloads": int(total),
        "distinct_downloads": int(len(freq)),
        "rank_identical": bool((old["rank"].to_numpy() == new["rank"].to_numpy()).all()),
        "cum_model_pct_max_abs_diff": float(
            np.abs(old["cum_model_pct"].to_numpy() - new["cum_model_pct"].to_numpy()).max()),
        "cum_download_pct_max_abs_diff": float(
            np.abs(old["cum_download_pct"].to_numpy()
                   - new["cum_download_pct"].to_numpy()).max()),
    }

    values = [int(v) for v in freq["downloads"]]
    counts = [int(c) for c in freq["freq"]]
    per_model = models["downloads"].to_numpy(dtype=float)
    g_rank = 2.0 * float(np.sum((np.arange(1, n + 1) - 0.5) * np.sort(per_model))) \
        / (n * total) - 1.0
    result["gini_per_model_rank"] = round(g_rank, 6)
    result["gini_per_model_cumsum"] = round(SU.gini_sorted_ascending(per_model), 6)
    result["gini_freq_weighted"] = round(SU.gini_freq_weighted(values, counts), 6)
    result["gini_max_abs_diff"] = max(
        abs(result["gini_per_model_rank"] - result["gini_freq_weighted"]),
        abs(result["gini_per_model_cumsum"] - result["gini_freq_weighted"]))
    result["gini_max_abs_diff"] = float(result["gini_max_abs_diff"])

    for p in (0.01, 0.05, 0.10):
        k = max(1, int(n * p))
        old_share = float(np.sort(per_model)[::-1][:k].sum() / total)
        new_share = float(SU.top_k_share(values, counts, p))
        result[f"top_{int(p * 100)}pct_old"] = round(old_share, 6)
        result[f"top_{int(p * 100)}pct_new"] = round(new_share, 6)
        result[f"top_{int(p * 100)}pct_abs_diff"] = abs(old_share - new_share)

    ok = (result["rank_identical"]
          and result["cum_model_pct_max_abs_diff"] == 0.0
          and result["cum_download_pct_max_abs_diff"] < 1e-9
          and result["gini_max_abs_diff"] < 1e-9
          and all(result[f"top_{int(p * 100)}pct_abs_diff"] < 1e-12 for p in (0.01, 0.05, 0.10)))
    result["equivalent"] = bool(ok)

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("\n[OK] 新频数聚合口径与旧全局窗口口径逐行等价"
          if ok else "\n[FAIL] 两种口径不等价，禁止用于全量")


if __name__ == "__main__":
    main()
