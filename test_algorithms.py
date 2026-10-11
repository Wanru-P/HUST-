#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_algorithms.py — 算法正确性单元测试（Phase 5 验收项）。

只测**纯计算**部分，不需要 Spark，因此可以随时快速回归：
  1. Gini 三种口径互相等价（秩式 / 频数加权 / 朴素两两差）
  2. Gini 从 Lorenz 曲线梯形积分反算 == 解析式（这是恒等式，不是近似）
  3. Top-p 份额与"排序后取前 k 个"的朴素定义一致，且与输入顺序无关（并列可复现）
  4. 精确分位数与 numpy.percentile(linear) 完全一致
  5. Lorenz / CCDF 点的单调性与端点
  6. crc32 选样规则的固定值回归（与 Spark F.crc32 的对拍在 test_spark_contracts.py）

Spark 侧契约（时间转换、Task Monthly 守恒、边方向、family size、train/test 不重叠、
图节点覆盖）在 `test_spark_contracts.py` 中，需要真实数据。

运行：python test_algorithms.py
"""
import sys

import numpy as np

import stats_utils as SU

FAILURES = []


def check(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAILURES.append(name)


def gini_naive(values):
    """朴素定义：G = Σ_i Σ_j |x_i − x_j| / (2·n²·mean)。O(n²)，只用于小数组。"""
    x = np.asarray(values, dtype=float)
    n = x.size
    if n == 0 or x.sum() <= 0:
        return 0.0
    diff = np.abs(x[:, None] - x[None, :]).sum()
    return float(diff / (2.0 * n * n * x.mean()))


def lorenz_area_gini(values_asc, freqs):
    """从 Lorenz 曲线梯形积分反算 Gini：G = 1 − 2·∫L(p)dp。

    对"同值分组"的频数表，组内累计量随模型数线性增长，
    因此梯形积分在分组点上**精确**成立，与解析式恒等。
    """
    pts = SU.lorenz_points(values_asc, freqs)
    area = 0.0
    for (x0, y0), (x1, y1) in zip(pts[:-1], pts[1:]):
        area += (y0 + y1) * (x1 - x0) / 2.0
    return 1.0 - 2.0 * area


def test_gini_equivalence():
    rng = np.random.default_rng(20261011)
    worst_a = worst_b = 0.0
    for _ in range(300):
        n = int(rng.integers(1, 400))
        style = rng.integers(0, 3)
        if style == 0:
            x = rng.integers(0, 1000, size=n)
        elif style == 1:
            x = (rng.pareto(1.2, size=n) * 100).astype("int64")
        else:
            x = np.zeros(n, dtype="int64")
        v, f = np.unique(x, return_counts=True)
        g_rank = SU.gini_sorted_ascending(x)
        g_freq = SU.gini_freq_weighted(list(v), list(f))
        g_area = lorenz_area_gini(list(v), list(f))
        worst_a = max(worst_a, abs(g_rank - g_freq))
        worst_b = max(worst_b, abs(g_rank - g_area))
    check("Gini 秩式 == 频数加权（300 组随机用例）", worst_a < 1e-12,
          f"max|Δ|={worst_a:.3e}")
    check("Gini 秩式 == Lorenz 梯形积分反算（300 组随机用例）", worst_b < 1e-12,
          f"max|Δ|={worst_b:.3e}")

    for arr, want in ([0, 0, 0], 0.0), ([5], 0.0), ([0, 100], 0.5), ([1, 1, 1, 1], 0.0):
        v, f = np.unique(np.array(arr), return_counts=True)
        got = SU.gini_freq_weighted(list(v), list(f))
        check(f"Gini 边界 {arr} == {want}", abs(got - want) < 1e-12, f"got={got}")

    small = [3, 1, 4, 1, 5, 9, 2, 6]
    v, f = np.unique(np.array(small), return_counts=True)
    got = SU.gini_freq_weighted(list(v), list(f))
    want = gini_naive(small)
    check("Gini 频数加权 == 朴素两两差定义", abs(got - want) < 1e-12,
          f"freq={got:.12f} naive={want:.12f}")


def test_top_k_share():
    rng = np.random.default_rng(7)
    worst = 0.0
    for _ in range(300):
        n = int(rng.integers(1, 500))
        x = rng.integers(0, 100, size=n)
        v, f = np.unique(x, return_counts=True)
        total = float(x.sum())
        for p in (0.01, 0.05, 0.10):
            k = max(1, int(n * p))
            want = float(np.sort(x)[::-1][:k].sum() / total) if total > 0 else None
            got = SU.top_k_share(list(v), list(f), p)
            if want is None:
                continue
            worst = max(worst, abs(got - want))
    check("Top-p 份额 == 排序取前 k 的朴素定义（300×3 用例）", worst < 1e-12,
          f"max|Δ|={worst:.3e}")

    base = np.array([0] * 60 + [1] * 20 + [10] * 15 + [1000] * 5)
    v, f = np.unique(base, return_counts=True)
    ref = SU.top_k_share(list(v), list(f), 0.10)
    order_stable = True
    for seed in range(20):
        shuffled = np.random.default_rng(seed).permutation(base)
        vs, fs = np.unique(shuffled, return_counts=True)
        if abs(SU.top_k_share(list(vs), list(fs), 0.10) - ref) > 1e-15:
            order_stable = False
    check("Top-p 份额与输入顺序无关（并列值可复现）", order_stable,
          f"ref={ref:.12f}")

    check("Top-p 全零数据返回 None",
          SU.top_k_share([0, 0, 0], [3], 0.10) is None)


def test_quantile():
    rng = np.random.default_rng(4242)
    worst = 0.0
    for _ in range(200):
        n = int(rng.integers(1, 300))
        x = rng.integers(0, 50, size=n)
        v, f = np.unique(x, return_counts=True)
        for q in (0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0):
            got = SU.quantile_from_freq(list(v), list(f), q)
            want = float(np.percentile(x, 100 * q))
            worst = max(worst, abs(got - want))
    check("quantile_from_freq == numpy.percentile(linear)", worst < 1e-12,
          f"max|Δ|={worst:.3e}")
    check("空输入分位数返回 None", SU.quantile_from_freq([], [], 0.5) is None)


def test_lorenz_ccdf():
    x = np.array([0] * 50 + [1] * 30 + [5] * 15 + [50] * 5)
    v, f = np.unique(x, return_counts=True)
    pts = SU.lorenz_points(list(v), list(f))
    check("Lorenz 起点为 (0,0)", pts[0] == (0.0, 0.0), str(pts[0]))
    check("Lorenz 终点为 (1,1)", abs(pts[-1][0] - 1.0) < 1e-12
          and abs(pts[-1][1] - 1.0) < 1e-12, str(pts[-1]))
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    check("Lorenz x 单调不减", all(b >= a for a, b in zip(xs, xs[1:])))
    check("Lorenz y 单调不减", all(b >= a for a, b in zip(ys, ys[1:])))
    check("Lorenz 曲线位于对角线上方（表示不平等）",
          all(y <= x + 1e-12 for x, y in pts))

    cc = SU.ccdf_points(list(v), list(f))
    check("CCDF 仅含正取值", all(c[0] > 0 for c in cc), f"{len(cc)} 点")
    check("CCDF 单调不增", all(b <= a + 1e-15 for (_, a), (_, b) in zip(cc, cc[1:])))
    check("CCDF 值域 (0,1]", all(0 < c[1] <= 1.0 for c in cc))
    # 强检验：逐点等于直接定义 P(X >= x) = (x >= v).mean()
    worst = max(abs(p - float((x >= a).mean())) for a, p in cc)
    check("CCDF 逐点 == 直接定义 P(X>=x)", worst < 1e-12, f"max|Δ|={worst:.3e}")
    check("CCDF 首点 == P(X>=最小正值)（含零值时为 <1）",
          abs(cc[0][1] - float((x >= cc[0][0]).mean())) < 1e-12, str(cc[0]))


def test_concentration_stats():
    x = np.array([0] * 100 + [7] * 40 + [12345] * 10)
    v, f = np.unique(x, return_counts=True)
    st = SU.concentration_stats(list(v), list(f))
    check("concentration_stats.n_models 正确", st["n_models"] == 150, str(st["n_models"]))
    check("concentration_stats.total_downloads 正确",
          st["total_downloads"] == 7 * 40 + 12345 * 10, str(st["total_downloads"]))
    check("concentration_stats 零值占比正确",
          abs(st["zero_value_share"] - round(100 / 150, 6)) < 1e-12,
          str(st["zero_value_share"]))
    check("concentration_stats 零值个数正确", st["zero_value_models"] == 100,
          str(st["zero_value_models"]))
    check("concentration_stats 唯一取值数正确", st["distinct_values"] == 3,
          str(st["distinct_values"]))
    check("concentration_stats.gini == gini_freq_weighted",
          abs(st["gini"] - round(SU.gini_freq_weighted(list(v), list(f)), 6)) < 1e-12)
    check("concentration_stats 三种 Top-k 齐备",
          all(k in st for k in ("top_1pct_share", "top_5pct_share", "top_10pct_share")))


def test_crc32_sampling_rule():
    """固定值回归：这些值已与 Spark F.crc32 实测对拍一致（见 docs/final_acceptance.md）。"""
    import zlib
    expected = {
        "bert-base-uncased": (1844169826, 26),
        "org/model-1": (32715943, 43),
        "black-forest-labs/FLUX.1-dev": (3699422153, 53),
        "中文/模型": (2235250147, 47),
        "datalab-to/surya-ocr-2-gguf": (1245776636, 36),
    }
    ok = True
    for s, (want_c, want_m) in expected.items():
        c = zlib.crc32(s.encode("utf-8"))
        if c != want_c or c % 100 != want_m:
            ok = False
            print(f"    MISMATCH {s}: got ({c}, {c % 100}) want ({want_c}, {want_m})")
    check("crc32 选样规则与 Spark 实测值一致", ok)

    n = 200000
    ids = [f"org{i}/model-{i}" for i in range(n)]
    keep = sum(1 for s in ids if zlib.crc32(s.encode("utf-8")) % 100 < 10)
    rate = keep / n
    check("crc32 % 100 < 10 的比例接近 10%", abs(rate - 0.10) < 0.005, f"rate={rate:.5f}")

    # 嵌套性：10% 集合必须包含在 25% 集合里（各档之间只差数据量，不差样本构成）
    sub10 = {i for i in range(5000) if zlib.crc32(f"m{i}".encode()) % 100 < 10}
    sub25 = {i for i in range(5000) if zlib.crc32(f"m{i}".encode()) % 100 < 25}
    check("10% 选样是 25% 选样的子集（嵌套设计）", sub10 <= sub25)


def main():
    test_gini_equivalence()
    test_top_k_share()
    test_quantile()
    test_lorenz_ccdf()
    test_concentration_stats()
    test_crc32_sampling_rule()
    print()
    if FAILURES:
        print(f"FAILED {len(FAILURES)} 项: {FAILURES}")
        sys.exit(1)
    print("全部算法单元测试通过")


if __name__ == "__main__":
    main()
