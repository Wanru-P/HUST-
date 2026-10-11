#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stats_utils.py — 长尾/集中度统计的**唯一口径实现**（Phase 1B）。

只放纯 Python / numpy 计算，不 import pyspark、pandas，供三条路径共用：
  - spark_etl.py          频数聚合后对**小频数表**做精确标量计算
  - local_etl.py          pandas 路径直接调用
  - test_gini.py          Gini 两式等价性与 Top-k 的单元验证

为什么单独抽出来：Phase 1A 发现同一个 Gini 在三处实现（analysis_evo /
analysis_rq5 / local_analysis）各写了一遍，容易出现口径漂移。
Phase 1B 起统一到这里，避免"同一指标不同数字"。

关键定义（全工程一致）：
  Gini（秩式，升序）：G = 2S/(n·total) − 1，S = Σ(rank−0.5)·x
  Gini（频数加权）：把上式按唯一值聚合后的等价形式（见 gini_freq_weighted 注释）
  Top-p 份额：按值降序取前 max(1, int(n·p)) 个模型；并列值整体同权，
             因此"取哪几个并列模型"不影响结果，算法可复现
"""
import numpy as np


def gini_sorted_ascending(values):
    """升序数组的基尼系数（累计式 (n+1−2Σcum/total)/n）。

    与秩式 2S/(n·total)−1 数学等价（test_gini.py 已用已知数组证明，最大绝对差 5.55e-16）。
    total ≤ 0（全零）时返回 0.0，避免除零。
    """
    arr = np.sort(np.asarray(values, dtype=float))
    n = arr.size
    total = float(arr.sum())
    if n == 0 or total <= 0:
        return 0.0
    cum = np.cumsum(arr)
    return float((n + 1 - 2.0 * (cum / total).sum()) / n)


def gini_freq_weighted(values_asc, freqs):
    """频数加权基尼系数（对"唯一值 + 频数"表，而不是把每个唯一值当模型）。

    推导：升序第 i 个唯一值 v_i 频数 f_i，它占据秩区间 (F_{i-1}, F_i]，
          Σ_{r=F_{i-1}+1}^{F_i} (r − 0.5) = f_i·(F_{i-1}+F_i)/2
    所以 S = Σ v_i·f_i·(F_{i-1}+F_i)/2，G = 2S/(n·total) − 1。
    与逐模型秩式**精确等价**，但输入规模从"模型数"降到"唯一值数"。
    """
    n = int(sum(freqs))
    total = float(sum(v * f for v, f in zip(values_asc, freqs)))
    if n == 0 or total <= 0:
        return 0.0
    prev = 0
    s = 0.0
    for v, f in zip(values_asc, freqs):
        s += float(v) * f * (2 * prev + f) / 2.0
        prev += f
    return 2.0 * s / (n * total) - 1.0


def top_k_share(values_asc, freqs, p):
    """Top-p 模型的下载份额。

    k = max(1, int(n·p))（与旧实现 max(1, int(n*p)) 一致）。
    按值从大到小累加频数，切到 k 的那个唯一值组按需部分计入——
    因为组内所有模型的值相同，部分计入不引入任意性，结果可复现。
    """
    n = int(sum(freqs))
    total = float(sum(v * f for v, f in zip(values_asc, freqs)))
    if n == 0 or total <= 0:
        return None
    k = max(1, int(n * p))
    acc = 0
    s = 0.0
    for v, f in zip(reversed(values_asc), reversed(freqs)):
        take = min(f, k - acc)
        s += take * float(v)
        acc += take
        if acc >= k:
            break
    return s / total


def lorenz_points(values_asc, freqs):
    """频数表 -> Lorenz 曲线点 [(cum_model_pct, cum_download_pct), ...]。

    每个点都是**真实累计值**（累计模型数占比 vs 累计下载占比），
    点的"减少"来自把同一下载量的模型合并成一个点，而不是抽样或插值。
    """
    n = int(sum(freqs))
    total = float(sum(v * f for v, f in zip(values_asc, freqs)))
    if n == 0 or total <= 0:
        return [(0.0, 0.0), (1.0, 1.0)]
    pts = [(0.0, 0.0)]
    cf = 0
    cv = 0.0
    for v, f in zip(values_asc, freqs):
        cf += int(f)
        cv += float(v) * int(f)
        pts.append((cf / n, cv / total))
    return pts


def ccdf_points(values_asc, freqs):
    """互补累积分布 P(X ≥ x) 的 (x, p) 点，供双对数长尾图使用（仅正 x）。"""
    n = int(sum(freqs))
    if n == 0:
        return []
    out = []
    remaining = n
    for v, f in zip(values_asc, freqs):
        if v > 0:
            out.append((float(v), remaining / n))
        remaining -= int(f)
    return out


def rank_value(values_asc, cum_freq, rank):
    """频数表上第 rank 个（1-based）模型对应的取值。"""
    idx = int(np.searchsorted(np.asarray(cum_freq), rank, side="left"))
    return float(np.asarray(values_asc, dtype=float)[idx])


def quantile_from_freq(values_asc, freqs, q):
    """频数表上的**精确**分位数，口径与 numpy.percentile 默认（linear）一致。

    h = (n−1)·q，取秩 floor(h)+1 与 ceil(h)+1 两个位置的值线性插值。
    这样 RQ5 的"中位下载量"不必用 percentile_approx 近似，也不会随并行度变化。
    """
    n = int(sum(freqs))
    if n == 0:
        return None
    h = (n - 1) * float(q)
    lo = int(np.floor(h))
    hi = int(np.ceil(h))
    cum = np.cumsum(np.asarray(freqs, dtype=np.int64))
    v_lo = rank_value(values_asc, cum, lo + 1)
    v_hi = rank_value(values_asc, cum, hi + 1)
    if hi == lo:
        return v_lo
    return v_lo + (v_hi - v_lo) * (h - lo)


def concentration_stats(values_asc, freqs):
    """频数表（值升序 + 频数）-> 集中度汇总字典。

    Spark 与 pandas 两条路径都调用这一个函数，保证同一数据上
    Gini / Top-k / 零值占比**逐位一致**，不会出现"两套实现两个数字"。
    """
    values = [int(v) for v in values_asc]
    counts = [int(c) for c in freqs]
    n = int(sum(counts))
    total = float(sum(v * c for v, c in zip(values, counts)))
    zero = int(sum(c for v, c in zip(values, counts) if v == 0))
    shares = {f"top_{int(p * 100)}pct_share": top_k_share(values, counts, p)
              for p in (0.01, 0.05, 0.10)}
    return {
        "n_models": n,
        "total_downloads": int(total),
        "distinct_values": len(values),
        "zero_value_models": zero,
        "zero_value_share": round(zero / n, 6) if n else None,
        "gini": round(gini_freq_weighted(values, counts), 6),
        **{k: (None if v is None else round(float(v), 6)) for k, v in shares.items()},
    }
