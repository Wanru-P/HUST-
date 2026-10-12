# -*- coding: utf-8 -*-
"""Part A · RQ2 三口径 Gini 稳健性验证（补充分析）。

从 ccdf.csv（每个下载量取值的互补累积分布）重建频数分布，计算三口径 Gini：
⚠️ 频数表由 **float ccdf + round** 反推，属「重建」而非位精确；脚本会打印重建总频数与期望值的差异。
  G_all 全量 / G_pos 剔除零下载 / G_top Top-1% 内部，并输出衰减曲线（用于 Figure S1）。

用法：
    python supplementary/analyze_gini.py
输入：data_hubstats/gold/analysis/ccdf.csv（可用环境变量 HF_CCDF_PATH 覆盖）
输出：supplementary/rq2_gini_result.json
依赖：仅 Python 标准库，无需 Spark、无需原始 parquet。
"""
import csv
import json
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CCDF_PATH = os.environ.get(
    "HF_CCDF_PATH",
    os.path.join(PROJECT_ROOT, "data_hubstats", "gold", "analysis", "ccdf.csv"))
OUT_DIR = os.environ.get("SUPP_OUT_DIR", os.path.join(PROJECT_ROOT, "supplementary"))
OUT_PATH = os.path.join(OUT_DIR, "rq2_gini_result.json")
N_EXPECT = 3138954            # 与 concentration_stats.json 的 n_models 一致
THRESHOLDS = [0, 1, 10, 100, 1000, 10000, 100000]


def load_metric(metric):
    rows = []
    with open(CCDF_PATH, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["metric"] == metric:
                rows.append((int(r["value"]), float(r["ccdf"])))
    rows.sort(key=lambda t: t[0])
    return rows


def reconstruct_counts(rows):
    """ccdf(v)=P(X>=v) 升序 -> [(value, count)]。补 value=0（ccdf(0)=1）。"""
    c0 = rows[0][1] if rows else 1.0
    counts = [(0, round(N_EXPECT * (1.0 - c0)))]
    for i, (v, c) in enumerate(rows):
        cnext = rows[i + 1][1] if i + 1 < len(rows) else 0.0
        counts.append((v, round(N_EXPECT * (c - cnext))))
    return counts


def gini(counts):
    """频数加权 Gini = sum x*n*(2*P + n - N) / (N*S)。
    输入为整数频数表、函数内为整数累加；但频数表本身由 float ccdf 重建，故整体并非位精确。"""
    counts = sorted(counts, key=lambda t: t[0])
    N = sum(n for _, n in counts)
    S = sum(x * n for x, n in counts)
    P = 0
    total = 0
    for x, n in counts:
        total += x * n * (2 * P + n - N)
        P += n
    return total / (N * S)


def gini_top_pct(counts, pct=0.01):
    counts = sorted(counts, key=lambda t: -t[0])
    N = sum(n for _, n in counts)
    # 与正式工程 stats_utils 统一口径：Top-p 取 int(N·p)，不用 round
    # （round 会在边界处多算/少算一个模型，导致与结果清单的 Top-1% 份额口径不一致）
    target = int(N * pct)
    picked, acc = [], 0
    for x, n in counts:
        if acc >= target:
            break
        take = n if acc + n <= target else target - acc
        if take > 0:
            picked.append((x, take))
        acc += take
    return gini(picked)


def decay(counts):
    return [{"t": t, "gini": round(gini([(x, n) for x, n in counts if x >= t]), 6)}
            for t in THRESHOLDS]


def main():
    result = {}
    for metric in ("downloads", "downloadsAllTime"):
        counts = reconstruct_counts(load_metric(metric))
        n_rec = sum(n for _, n in counts)
        if n_rec != N_EXPECT:
            print(f"  [warn] {metric}: 重建总频数 {n_rec:,} != 期望 {N_EXPECT:,}（差 {n_rec - N_EXPECT:+,}）")
        g_all = gini(counts)
        g_pos = gini([(x, n) for x, n in counts if x > 0])
        g_top = gini_top_pct(counts)
        result[metric] = {
            "g_all": round(g_all, 6),
            "g_pos": round(g_pos, 6),
            "g_top": round(g_top, 6),
            "n_reconstructed": int(n_rec),
            "n_expected": int(N_EXPECT),
            "n_top1pct": int(N_EXPECT * 0.01),
            "decay": decay(counts),
        }
        print(f"[{metric}] n={n_rec:,}  G_all={g_all:.6f}  G_pos={g_pos:.6f}  G_top={g_top:.6f}")
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"-> {OUT_PATH}")


if __name__ == "__main__":
    main()
